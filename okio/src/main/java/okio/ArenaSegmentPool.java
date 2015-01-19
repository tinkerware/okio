package okio;

import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

import org.codehaus.mojo.animal_sniffer.IgnoreJRERequirement;

import okio.pool.PoolMetrics;

/**
 * A segment pool that allocates segments from thread-local arenas.
 */
@IgnoreJRERequirement
class ArenaSegmentPool implements AllocatingPool {

  static final ArenaSegmentPool INSTANCE = new ArenaSegmentPool();

  /**
   * A separate region of memory used to allocate and recycle segments. An arena
   * is implicitly tied to a single thread, known as its local thread. Arena's
   * associated with other threads are siblings.
   */
  private class Arena {
    /**
     * The pool is only modified at the head by the arena's thread. Other arenas
     * may steal free segments from the tail.
     */
    private final ConcurrentLinkedDeque<Segment> pool = new ConcurrentLinkedDeque<>();

    /**
     * The total number of free bytes currently in the pool; never negative.
     */
    private final AtomicLong byteCount = new AtomicLong();

    @IgnoreJRERequirement
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
    @IgnoreJRERequirement
    Segment take() {
      Segment segment = pool.pollFirst();
      if (segment != null) {
        return recordReusedSegment(segment);
      }

      return stealOrAllocate();
    }

    private Segment stealOrAllocate() {
      Segment result = randomArena().steal();
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
    @IgnoreJRERequirement
    void recycleLocal(Segment segment) {
      if (byteCount.get() >= AllocatingPool.MAX_SIZE) {
        // TODO: This is a good place to notify a periodic reaper.
        // That would prevent wastage from a large number of threads
        // each holding on to maximum allowed number of segments.
        recorder.recordRecycle(Segment.SIZE, true);
        return;
      }
      pool.offerFirst(segment);
      byteCount.addAndGet(Segment.SIZE);
      recorder.recordRecycle(Segment.SIZE, false);
    }

    /**
     * Takes a segment from the tail. If the pool is empty or this arena is closed,
     * returns null. Called from a sibling's thread.
     */
    @IgnoreJRERequirement
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

    @IgnoreJRERequirement
    private Arena randomArena() {
      int index = ThreadLocalRandom.current().nextInt(allArenas.size());
      Arena result;
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

    @Override public String toString() {
      return String.format("Arena[tid=%s, identity=%s, size=%,3d]",
                           Long.toHexString(Thread.currentThread().getId()),
                           super.toString(),
                           byteCount.get());
    }
  }

  private final ThreadLocal<Arena> arena = new ThreadLocal<Arena>() {
    @Override protected Arena initialValue() {
      Arena arena = new Arena();
      allArenas.add(arena);
      return arena;
    }

    @Override public void remove() {
      allArenas.remove(get());
      super.remove();
    }
  };

  private final CopyOnWriteArrayList<Arena> allArenas = new CopyOnWriteArrayList<>();

  private final PoolMetrics.Recorder recorder = new PoolMetrics.Recorder();

  @Override public Segment take() {
    return arena.get().take();
  }

  @Override public void recycle(Segment segment) {
    if (segment.next != null || segment.prev != null) throw new IllegalArgumentException();
    segment.next = null;
    segment.pos = segment.limit = 0;

    arena.get().recycleLocal(segment);
  }

  @Override public PoolMetrics metrics() {
    return recorder.snapshot();
  }
}
