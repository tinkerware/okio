package okio;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.codehaus.mojo.animal_sniffer.IgnoreJRERequirement;

import okio.pool.MetricsRecorder;
import okio.pool.PoolMetrics;
import okio.pool.RecorderSet;

/**
 * A segment pool that allocates segments from thread-local arenas.
 */
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
    executor.setKeepAliveTime(ARENA_CLEAN_PERIOD_MILLIS * 2, TimeUnit.MILLISECONDS);
    executor.allowCoreThreadTimeOut(true);

    scheduler = Executors.unconfigurableScheduledExecutorService(executor);
  }

  private static boolean checkRuntime() {
    return true;
  }

  /**
   * Arenas track their segments; a segment is always recycled at its owner arena. This avoids a
   * pitfall of thread-local allocators where less busy threads (or worse, low priority background
   * tasks) may consume a buffer created by a busy thread and end up recycling segments on their own
   * arenas, away from where they are needed. Also, we get to avoid a thread-local lookup on
   * recycle.
   */
  private static class ArenaSegment extends Segment {

    final ArenaRef arena;

    ArenaSegment(Arena arena) {
      this.arena = arena.ref();
    }
  }

  private static class ArenaMetrics {

    private final RecorderSet recorders = new RecorderSet();
    private final MetricsRecorder local = recorders.singleWriterRecorder();
    private final MetricsRecorder siblings = recorders.multiWriterRecorder();

    /**
     * The total number of free bytes currently in the arena; never negative.
     */
    @SuppressWarnings("UnusedDeclaration")
    private volatile long byteCount;
    @SuppressWarnings("UnusedDeclaration")
    private volatile long totalTrimmed;

    long byteCount() {
      return byteCount;
    }

    Segment recordReusedSegment(Segment segment) {
      return recordTakenSegment(segment, local);
    }

    Segment recordStolenSegment(Segment segment) {
      return recordTakenSegment(segment, siblings);
    }

    private Segment recordTakenSegment(Segment segment, MetricsRecorder recorder) {
      if (segment != null) {
        recorder.recordUse(Segment.SIZE, false);
        long updatedArenaSize = atomicBytes.addAndGet(this, -Segment.SIZE);
        assert updatedArenaSize >= 0 : String.format("updatedArenaSize >= 0 : %d, %s",
                                                     updatedArenaSize,
                                                     toString());
      }
      return segment;
    }

    void recordLocalRecycledSegment(int segmentSize, boolean deallocated) {
      if (!deallocated) {
        atomicBytes.getAndAdd(this, segmentSize);
      }

      local.recordRecycle(segmentSize, deallocated);
    }

    void recordSiblingRecycledSegment(int segmentSize, boolean deallocated) {
      if (!deallocated) {
        atomicBytes.getAndAdd(this, segmentSize);
      }

      siblings.recordRecycle(segmentSize, deallocated);
    }

    void recordAllocation(int segmentSize) {
      local.recordUse(segmentSize, true);
    }

    long recordTrim(int trimmedSize) {
      atomicTrimmed.getAndAdd(this, trimmedSize);
      return atomicBytes.addAndGet(this, -trimmedSize);
    }

    PoolMetrics snapshot() {
      return recorders.aggregate();
    }

    private static final AtomicLongFieldUpdater<ArenaMetrics> atomicBytes =
        AtomicLongFieldUpdater.newUpdater(ArenaMetrics.class, "byteCount");
    private static final AtomicLongFieldUpdater<ArenaMetrics> atomicTrimmed =
        AtomicLongFieldUpdater.newUpdater(ArenaMetrics.class, "totalTrimmed");
  }

  /**
   * A separate region of memory used to allocate and recycle segments. An arena is implicitly tied
   * to a single thread, known as its local thread. Arena's associated with other threads are
   * siblings.
   */
  @IgnoreJRERequirement
  class Arena {

    /**
     * The pool is only modified at the head by the arena's thread. Other arenas may steal free
     * segments from the tail.
     */
    private final Deque<ArenaSegment> pool = new ConcurrentLinkedDeque<>();

    private final long threadId = Thread.currentThread().getId();

    private final Random random = new Random();

    private final ArenaMetrics metrics = new ArenaMetrics();

    private final ArenaRef selfRef;

    Arena() {
      this.selfRef = new ArenaRef(this);
    }

    ArenaRef ref() {
      return selfRef;
    }

    /**
     * Reserves a free segment or allocates a new segment. An arena first checks its own pool, then
     * randomly picks another arena and steals its tail. Only called from the local thread. This
     * method handles the fast path so it can be inlined more easily; it delegates to {@linkplain
     * #stealOrAllocate()} to steal from a sibling or allocate a fresh segment.
     */
    Segment take() {
      Segment segment = pool.pollFirst();
      if (segment != null) {
        return metrics.recordReusedSegment(segment);
      }

      return stealOrAllocate();
    }

    private Segment stealOrAllocate() {
      Segment result = randomArena().stealSegment(this);
      if (result == null) {
        result = new ArenaSegment(this);
        metrics.recordAllocation(Segment.SIZE);
      }
      return result;
    }

    /**
     * Recycles a segment to be free so that it can be reserved again at a later time. The recycled
     * segment is enqueued at the head of the queue; we will never contend with same-thread takes
     * and only rarely have to contend with siblings.
     */
    void recycleLocal(ArenaSegment segment) {
      if (metrics.byteCount() >= ARENA_SIZE_HIGHMARK) {
        metrics.recordLocalRecycledSegment(Segment.SIZE, true);
        return;
      }

      pool.offerFirst(segment);
      metrics.recordLocalRecycledSegment(Segment.SIZE, false);
    }

    /**
     * Same as above, except it appends the tail so that it doesn't contend with local usage.
     */
    void recycleSibling(ArenaSegment segment) {
      if (metrics.byteCount() >= ARENA_SIZE_HIGHMARK) {
        metrics.recordSiblingRecycledSegment(Segment.SIZE, true);
        return;
      }

      pool.offerLast(segment);
      metrics.recordSiblingRecycledSegment(Segment.SIZE, false);
    }

    // Visible for testing.
    int segmentCount() {
      return pool.size();
    }

    /**
     * Takes a segment from the tail. If the pool is empty or this arena is closed, returns null.
     * Called from a sibling's thread.
     */
    private Segment steal() {
      return metrics.recordStolenSegment(pool.pollLast());
    }

    private ArenaRef randomArena() {
      int index = random.nextInt(allArenas.size());
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

    /**
     * Trim an arena so that its size is at most {@code lowmark}. May be called concurrently.
     */
    private void maybeTrim(long lowMark, long highMark) {
      long currentSize = metrics.byteCount();
      if (currentSize >= highMark) {
        // Let's shrink the pool until size is smaller than the low mark.
        while (metrics.recordTrim(Segment.SIZE) >= lowMark) {
          pool.pollLast();
        }
      }
    }

    @Override public String toString() {
      return String.format("Arena[tid=%s, identity=%s, size=%,3d]",
                           Long.toHexString(threadId),
                           super.toString(),
                           metrics.byteCount());
    }
  }

  /**
   * A weak reference to an arena so that the pool can avoid holding on to arenas whose threads are
   * dead.
   */
  private static class ArenaRef extends WeakReference<Arena> {

    private final ArenaMetrics metrics;

    public ArenaRef(Arena enclosing) {
      super(enclosing);
      this.metrics = enclosing.metrics;
    }

    Segment stealSegment(Arena thief) {
      Arena arena = get();
      return arena != null && arena != thief ? arena.steal() : null;
    }

    boolean recycleSegment(ArenaSegment segment) {
      if (this != segment.arena) throw new IllegalArgumentException("this != segment.arena");
      Arena arena = get();
      if (arena != null) {
        long threadId = Thread.currentThread().getId();
        if (arena.threadId == threadId) {
          arena.recycleLocal(segment);
        } else {
          arena.recycleSibling(segment);
        }
        return true;
      }
      return false;
    }

    void maybeTrim(long lowmark, long highmark) {
      Arena arena = get();
      if (arena != null) {
        arena.maybeTrim(lowmark, highmark);
      }
    }

    PoolMetrics metrics() {
      return metrics.snapshot();
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

  interface ArenaCallback {
    ArenaCallback NONE = new ArenaCallback() {
      @Override public void onArena(ArenaRef arena) {
        // nothing
      }
    };

    void onArena(ArenaRef arena);
  }

  private void sweepArenas(ArenaCallback alive, ArenaCallback reclaimed) {
    synchronized (allArenas) {
      Collection<ArenaRef> refsToRemove = new ArrayList<>();
      for (ArenaRef arenaRef : allArenas) {
        Arena arena = arenaRef.get();
        if (arena == null) {
          refsToRemove.add(arenaRef);
        }
        else {
          alive.onArena(arenaRef);
        }
      }

    /*
     * We need to avoid clobbering the baseline globalMetrics, which contain the
     * metric values of all arenas that have been reclaimed. We ensure this
     * by allowing any potentially concurrent clean tasks to update the
     * set of arenas and baseline globalMetrics only atomically.
     */
      if (!refsToRemove.isEmpty() && allArenas.removeAll(refsToRemove)) {
        for (ArenaRef arena : refsToRemove) {
          reclaimed.onArena(arena);
        }
      }
    }
  }

  private void cleanArenas(final long lowmark, final long highmark) {
    sweepArenas(new ArenaCallback() {
      @Override public void onArena(ArenaRef arena) {
        arena.maybeTrim(lowmark, highmark);
      }
    }, new ArenaCallback() {
      @Override public void onArena(ArenaRef arena) {
        globalMetrics.recordReclaimed(arena.metrics());
      }
    });
  }

  class GlobalMetrics {
    private final AtomicLong discarded = new AtomicLong();
    private volatile PoolMetrics baselineMetrics = PoolMetrics.zero();

    // Guarded by this GlobalMetrics instance.
    private final List<PoolMetrics> previouslyReclaimed = new LinkedList<>();

    class CollectingCallback implements ArenaCallback {

      private final Collection<PoolMetrics> collection;

      public CollectingCallback(Collection<PoolMetrics> collection) {
        this.collection = collection;
      }

      @Override public void onArena(ArenaRef arena) {
        collection.add(arena.metrics());
      }
    }

    public PoolMetrics aggregate() {
      final Collection<PoolMetrics> aliveArenas = new ArrayList<>();
      final Collection<PoolMetrics> reclaimedArenas = resetReclaimed();

      ArenaCallback aliveCallback = new CollectingCallback(aliveArenas);
      ArenaCallback reclaimedCallback = new CollectingCallback(reclaimedArenas);
      sweepArenas(aliveCallback, reclaimedCallback);

      PoolMetrics aliveMetrics = PoolMetrics.merge(aliveArenas);
      PoolMetrics reclaimedMetrics = PoolMetrics.merge(reclaimedArenas);

      /*
       * We reset the discarded counter to avoid open-ended iteration
       * in a degenerate environment where discarded segments are common.
       */
      MetricsRecorder recorder = MetricsRecorder.singleWriterRecorder();
      for (long discards = discarded.getAndSet(0); discards > 0; discards--) {
        recorder.recordRecycle(Segment.SIZE, true);
      }
      PoolMetrics discardedMetrics = recorder.snapshot();

      /*
       * We are assured that the combination of reclaimed and discarded metrics
       * is a delta (disjoint with any other deltas in concurrent aggregations)
       * due to the atomic sweep of arenas and atomic reset of the discarded
       * counter; otherwise merge would count duplicates.
       */
      PoolMetrics baselineDelta = reclaimedMetrics.merge(discardedMetrics);
      synchronized (this) {
        baselineMetrics = baselineMetrics.merge(baselineDelta);
        return aliveMetrics.merge(baselineMetrics);
      }
    }

    long recordDiscarded() {
      return discarded.incrementAndGet();
    }

    synchronized void recordReclaimed(PoolMetrics snapshot) {
      previouslyReclaimed.add(snapshot);
    }

    synchronized List<PoolMetrics> resetReclaimed() {
      List<PoolMetrics> result = new ArrayList<>(previouslyReclaimed);
      previouslyReclaimed.clear();
      return result;
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

  private final GlobalMetrics globalMetrics = new GlobalMetrics();

  private volatile boolean running = true;

  // Visible for testing.
  ArenaSegmentPool() {
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
    if (segment == null) throw new NullPointerException("segment");

    // We tolerate foreign segments to avoid hard-to-debug issues if
    // buffers created prior to a Util.installPool call were used
    // alongside buffers created later.
    if (!(running && segment instanceof ArenaSegment)) return;

    ArenaSegment arenaSegment = (ArenaSegment) segment;
    segment.reset();
    if (!arenaSegment.arena.recycleSegment(arenaSegment)) {
      // Arena has been reclaimed, discard this segment.
      globalMetrics.recordDiscarded();
    }
  }

  @Override public PoolMetrics metrics() {
    return globalMetrics.aggregate();
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
          Error error = new AssertionError("Pool worker is done");
          error.initCause(e);
          throw error;
        }
        catch (ExecutionException e) {
          thrown = e;
        }
        logger.log(Level.WARNING,
                   "Found previous pool worker failure; continuing pool shut down",
                   thrown);
      }
      else {
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
