package okio;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.codehaus.mojo.animal_sniffer.IgnoreJRERequirement;

import okio.pool.PoolMetrics;

import static java.util.Objects.requireNonNull;

/**
 * A segment pool that allocates segments from thread-local arenas.
 */
@IgnoreJRERequirement
class ArenaSegmentPool implements AllocatingPool {
  private static final Logger logger = Logger.getLogger(ArenaSegmentPool.class.getName());

  static final long ARENA_SIZE_HIGHMARK = AllocatingPool.MAX_SIZE;

  static final long ARENA_SIZE_LOWMARK = ARENA_SIZE_HIGHMARK / 2;

  static final int ARENA_CLEAN_PERIOD_MILLIS = 5 * 1000;

  private static final ScheduledExecutorService scheduler;

  static {
    ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1, new ThreadFactory() {

      @Override public Thread newThread(Runnable r) {
        Thread thread = new Thread(r, "Okio Pool Worker");
        thread.setDaemon(true);
        return thread;
      }
    });

    // We make sure the scheduler can be shutdown during finalization.
    executor.setRemoveOnCancelPolicy(true);
    executor.setKeepAliveTime(ARENA_CLEAN_PERIOD_MILLIS * 2, TimeUnit.MILLISECONDS);
    executor.allowCoreThreadTimeOut(true);

    scheduler = Executors.unconfigurableScheduledExecutorService(executor);
  }

  private static boolean checkRuntime() {
    try {
      Class.forName("java.util.concurrent.ThreadLocalRandom");
      return true;
    }
    catch (ClassNotFoundException e) {
      return false;
    }
  }

  /**
   * Arenas track their segments; a segment is always recycled at its owner
   * arena. This avoids a pitfall of thread-local allocators where less busy
   * threads (or worse, low priority background tasks) may consume a buffer
   * created by a busy thread and end up recycling segments on their own
   * arenas, away from where they are needed. Also, we get to avoid a
   * thread-local lookup on recycle.
   */
  private static class ArenaSegment extends Segment {
    final ArenaRef arena;

    ArenaSegment(Arena arena) {
      this.arena = arena.ref();
    }
  }

  /**
   * A separate region of memory used to allocate and recycle segments. An arena
   * is implicitly tied to a single thread, known as its local thread. Arena's
   * associated with other threads are siblings.
   */
  @IgnoreJRERequirement
  class Arena {
    /**
     * The pool is only modified at the head by the arena's thread. Other arenas
     * may steal free segments from the tail.
     */
    private final Deque<ArenaSegment> pool = new ConcurrentLinkedDeque<>();

    /**
     * The total number of free bytes currently in the pool; never negative.
     */
    private final AtomicLong byteCount = new AtomicLong();

    private final long threadId = Thread.currentThread().getId();

    private final ArenaRef selfRef;

    Arena() {
      this.selfRef = new ArenaRef(this);
    }

    ArenaRef ref() {
      return selfRef;
    }

    /**
     * Reserves a free segment or allocates a new segment. An arena first
     * checks its own pool, then randomly picks another arena and steals
     * its tail. Only called from the local thread.
     * This method handles the fast path so it can be inlined more easily;
     * it delegates to {@linkplain #stealOrAllocate()} to steal from a sibling
     * or allocate a fresh segment.
     */
    Segment take() {
      Segment segment = pool.pollFirst();
      if (segment != null) {
        return recordReusedSegment(segment);
      }

      return stealOrAllocate();
    }

    private Segment stealOrAllocate() {
      Segment result = randomArena().stealSegment(this);
      if (result == null) {
        result = new ArenaSegment(this);
        recorder.recordUse(Segment.SIZE, true);
      }
      return result;
    }

    /**
     * Recycles a segment to be free so that it can be reserved again at a later time.
     * The recycled segment is enqueued at the head of the queue; we will never contend
     * with same-thread takes and only rarely have to contend with siblings.
     */
    void recycle(ArenaSegment segment) {
      if (byteCount.get() >= ARENA_SIZE_HIGHMARK) {
        recorder.recordRecycle(Segment.SIZE, true);
        return;
      }

      if (inLocalThread()) {
        pool.offerFirst(segment);
      }
      else {
        pool.offerLast(segment);
      }
      byteCount.addAndGet(Segment.SIZE);
      recorder.recordRecycle(Segment.SIZE, false);
    }

    private boolean inLocalThread() {
      return threadId == Thread.currentThread().getId();
    }

    // Visible for testing.
    int segmentCount() {
      return pool.size();
    }

    /**
     * Takes a segment from the tail. If the pool is empty or this arena is closed,
     * returns null. Called from a sibling's thread.
     */
    private Segment steal() {
      return recordReusedSegment(pool.pollLast());
    }

    private Segment recordReusedSegment(Segment segment) {
      if (segment != null) {
        recorder.recordUse(Segment.SIZE, false);
        long updatedPoolSize = byteCount.addAndGet(-Segment.SIZE);
        assert updatedPoolSize >= 0 : String.format("updatedPoolSize >= 0 : %d, %s",
                                                    updatedPoolSize,
                                                    toString());
      }
      return segment;
    }

    private ArenaRef randomArena() {
      int index = ThreadLocalRandom.current().nextInt(allArenas.size());
      ArenaRef result;
      try {
        result = allArenas.get(index);
      }
      catch (IndexOutOfBoundsException e) {
        // We are called from inside an arena; there should be at least one.
        assert !allArenas.isEmpty() : "!allArenas.isEmpty";
        result = allArenas.get(0);
      }
      return result;
    }

    private void maybeTrim(long lowMark, long highMark) {
      long currentSize = byteCount.get();
      if (currentSize >= highMark) {
        // Let's shrink the pool until size is smaller than the low mark.
        while (byteCount.getAndAdd(-Segment.SIZE) >= lowMark) {
          pool.pollLast();
          recorder.recordTrim(Segment.SIZE);
        }
      }
    }

    @Override public String toString() {
      return String.format("Arena[tid=%s, identity=%s, size=%,3d]",
                           Long.toHexString(Thread.currentThread().getId()),
                           super.toString(),
                           byteCount.get());
    }
  }

  /**
   * A weak reference to an arena so that the pool can avoid holding on to
   * arenas whose threads are dead.
   */
  private static class ArenaRef extends WeakReference<Arena> {

    public ArenaRef(Arena enclosing) {
      super(enclosing);
    }

    Segment stealSegment(Arena thief) {
      Arena arena = get();
      return arena != null && arena != thief ? arena.steal() : null;
    }

    boolean recycleSegment(ArenaSegment segment) {
      if (this != segment.arena) throw new IllegalArgumentException("this != segment.arena");
      Arena arena = get();
      if (arena != null) {
        arena.recycle(segment);
        return true;
      }
      return false;
    }
  }

  class CleanTask implements Runnable {

    @Override public void run() {
      cleanArenas(ARENA_SIZE_LOWMARK, ARENA_SIZE_HIGHMARK);
    }

    public ScheduledFuture<?> schedule() {
      return scheduler.scheduleWithFixedDelay(this,
                                              ARENA_CLEAN_PERIOD_MILLIS,
                                              ARENA_CLEAN_PERIOD_MILLIS,
                                              TimeUnit.MILLISECONDS);
    }

  }

  private void cleanArenas(long lowmark, long highmark) {
    Collection<ArenaRef> refsToRemove = new ArrayList<>();
    for (ArenaRef arenaRef : allArenas) {
      Arena arena = arenaRef.get();
      if (arena == null) {
        refsToRemove.add(arenaRef);
      } else {
        arena.maybeTrim(lowmark, highmark);
      }
    }
    if (!refsToRemove.isEmpty()) {
      allArenas.removeAll(refsToRemove);
    }
  }

  // Visible for testing.
  final ThreadLocal<Arena> arena = new ThreadLocal<Arena>() {
    @Override protected Arena initialValue() {
      Arena arena = createArena();
      allArenas.add(arena.ref());
      return arena;
    }

  };

  private final ScheduledFuture<?> taskHandle;

  private final List<ArenaRef> allArenas = new CopyOnWriteArrayList<>();

  private final PoolMetrics.Recorder recorder = new PoolMetrics.Recorder();

  private volatile boolean running = true;

  // Visible for testing.
  ArenaSegmentPool() {
    if (!checkRuntime()) {
      throw new UnsupportedOperationException("ArenaSegmentPool requires Java 7");
    }
    this.taskHandle = createCleanTask().schedule();
  }

  // Visible for testing.
  CleanTask createCleanTask() {
    return new CleanTask();
  }

  //Visible for testing.
  Arena createArena() {
    return new Arena();
  }

  int arenaCount() {
    return allArenas.size();
  }

  @Override public Segment take() {
    checkState();
    return arena.get().take();
  }

  @Override public void recycle(Segment segment) {
    segment.reset();

    // We tolerate foreign segments to avoid hard-to-debug issues if
    // buffers created prior to a Util.installPool call were used
    // alongside buffers created later.
    if (!(running && requireNonNull(segment) instanceof ArenaSegment)) return;

    ArenaSegment arenaSegment = (ArenaSegment) segment;
    if (!arenaSegment.arena.recycleSegment(arenaSegment)) {
      // Arena has been reclaimed, discard this segment.
      recorder.recordRecycle(Segment.SIZE, true);
    }
  }

  @Override public PoolMetrics metrics() {
    return recorder.snapshot();
  }

  @Override public void shutdown() {
    running = false;

    if (taskHandle != null && !taskHandle.isCancelled()) {
      if (taskHandle.isDone()) {
        // Cleaner task had failed and we are now noticing.
        Throwable thrown = null;
        try {
          taskHandle.get();
        }
        catch (InterruptedException e) {
          throw new AssertionError("Pool worker is done", e);
        }
        catch (ExecutionException e) {
          thrown = e;
        }
        logger.log(Level.WARNING,
                   "Found previous pool worker failure; continuing pool shut down",
                   thrown);
      } else {
        taskHandle.cancel(false);
      }

      // Release all segments for GC and remove stale arena references.
      cleanArenas(0, 0);

      if (!allArenas.isEmpty()) {
        logger.warning("Threads that used Okio must die before this pool is reclaimed by GC.");
      }
    }
  }

  private void checkState() {
    if (!running) throw new IllegalStateException("!running");
  }
}
