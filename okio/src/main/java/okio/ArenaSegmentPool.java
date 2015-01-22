package okio;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.codehaus.mojo.animal_sniffer.IgnoreJRERequirement;

import okio.pool.PoolMetrics;

/**
 * A segment pool that allocates segments from thread-local arenas.
 */
@IgnoreJRERequirement
class ArenaSegmentPool implements AllocatingPool {

  static final long ARENA_SIZE_HIGHMARK = AllocatingPool.MAX_SIZE;

  static final long ARENA_SIZE_LOWMARK = ARENA_SIZE_HIGHMARK / 2;

  static final int ARENA_CLEAN_PERIOD_MILLIS = 5 * 1000;

  private static final ScheduledExecutorService scheduler;

  static {
    scheduler = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
      final AtomicInteger id = new AtomicInteger();
      @Override public Thread newThread(Runnable r) {
        Thread thread = new Thread(r, "Okio Arena Cleaner - " + id.incrementAndGet());
        thread.setDaemon(true);
        return thread;
      }
    });
  }

  private static boolean checkRuntime() {
    try {
      Class.forName("java.util.concurrent.ConcurrentLinkedDeque");
      return true;
    }
    catch (ClassNotFoundException e) {
      return false;
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
    private final ConcurrentLinkedDeque<Segment> pool = new ConcurrentLinkedDeque<>();

    /**
     * The total number of free bytes currently in the pool; never negative.
     */
    private final AtomicLong byteCount = new AtomicLong();

    Arena() {
      // constructor exists as target for the ignore JDK annotation only
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
        result = new Segment();
        recorder.recordUse(Segment.SIZE, true);
      }
      return result;
    }

    /**
     * Recycles a segment to be free so that it can be reserved again at a later time.
     * The recycled segment is pushed on the front of the queue in order to avoid
     * contending with siblings stealing and recycling surplus segments.
     */
    void recycleLocal(Segment segment) {
      if (byteCount.get() >= ARENA_SIZE_HIGHMARK) {
        recorder.recordRecycle(Segment.SIZE, true);
        return;
      }
      pool.offerFirst(segment);
      byteCount.addAndGet(Segment.SIZE);
      recorder.recordRecycle(Segment.SIZE, false);
    }

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

  private static class ArenaRef extends WeakReference<Arena> {

    public ArenaRef(Arena referent) {
      super(referent);
    }

    Segment stealSegment(Arena thief) {
      Arena arena = get();
      return arena != null && arena != thief ? arena.steal() : null;
    }

  }

  class CleanTask implements Runnable {

    @Override public void run() {
      Collection<ArenaRef> refsToRemove = new ArrayList<>();
      for (ArenaRef arenaRef : allArenas) {
        Arena arena = arenaRef.get();
        if (arena == null) {
          refsToRemove.add(arenaRef);
        } else {
          arena.maybeTrim(ARENA_SIZE_LOWMARK, ARENA_SIZE_HIGHMARK);
        }
      }
      if (!refsToRemove.isEmpty()) {
        allArenas.removeAll(refsToRemove);
      }
    }

    public ScheduledFuture<?> schedule() {
      return scheduler.scheduleWithFixedDelay(this,
                                              ARENA_CLEAN_PERIOD_MILLIS,
                                              ARENA_CLEAN_PERIOD_MILLIS,
                                              TimeUnit.MILLISECONDS);
    }

  }

  // Visible for testing.
  final ThreadLocal<Arena> arena = new ThreadLocal<Arena>() {
    @Override protected Arena initialValue() {
      Arena arena = createArena();
      allArenas.add(new ArenaRef(arena));
      return arena;
    }

  };

  private final CopyOnWriteArrayList<ArenaRef> allArenas = new CopyOnWriteArrayList<>();

  private final PoolMetrics.Recorder recorder = new PoolMetrics.Recorder();

  // Visible for testing.
  ArenaSegmentPool() {
    if (!checkRuntime()) {
      throw new UnsupportedOperationException("ArenaSegmentPool requires Java 7");
    }
    createCleanTask().schedule();
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
    return arena.get().take();
  }

  @Override public void recycle(Segment segment) {
    segment.reset();
    arena.get().recycleLocal(segment);
  }

  @Override public PoolMetrics metrics() {
    return recorder.snapshot();
  }
}
