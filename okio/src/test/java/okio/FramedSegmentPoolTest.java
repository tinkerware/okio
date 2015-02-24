package okio;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ScheduledFuture;

import org.junit.Test;

import okio.pool.PoolMetrics;

import static org.hamcrest.number.OrderingComparison.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import static okio.Util.currentPool;

public class FramedSegmentPoolTest {

  private static class NonSweepingPool extends FramedSegmentPool {

    private SweepTask task;

    @Override SweepTask createSweepTask() {
      task = new SweepTask() {
        @Override public ScheduledFuture<?> schedule() {
          return null;
        }
      };
      return task;
    }

    @Override boolean elapsedTimeSinceLastSweepTooLong() {
      return false;
    }
  }

  @Test public void noLeaksAfterThreadDeath() throws Exception {

    final NonSweepingPool pool = new NonSweepingPool();
    final Exception[] error = new Exception[1];
    Thread t = new Thread(new Runnable() {
      @Override public void run() {
        try {
          pool.recycle(pool.take());
        } catch (Exception e) {
          error[0] = e;
        }
      }
    });

    t.start();
    t.join();
    if (error[0] != null) {
      throw error[0];
    }

    // We should still have the cache reference after its thread dies.
    assertEquals(1, pool.cacheCount());
    // Let's give GC a chance to process weak references.
    System.gc();
    // Now we can expunge our stale entries.
    pool.task.run();
    // We should have no references to the cache of the dead thread.
    assertEquals("cache count", 0, pool.cacheCount());

  }

  @Test public void fillAndDrainPool() throws Exception {
    NonSweepingPool pool = new NonSweepingPool();
    List<Segment> segments = new ArrayList<>();

    PoolMetrics before, after;

    // Take 2 * MAX_SIZE segments to drain the pool.
    segments.addAll(takeSegments(pool, AllocatingPool.MAX_SIZE));
    segments.addAll(takeSegments(pool, AllocatingPool.MAX_SIZE));
    after = pool.latestCounters();

    assertEquals("drain: outstanding",
                 2 * AllocatingPool.MAX_SIZE,
                 after.outstandingByteCount());
    assertEquals("drain: used", 0, after.usedByteCount());

    // Recycle MAX_SIZE segments. They're all in the pool.
    recycleSegments(pool, segments.subList(0, segments.size() / 2));
    after = pool.latestCounters();

    assertEquals("recycle: outstanding",
                 AllocatingPool.MAX_SIZE,
                 after.outstandingByteCount());

    // Recycle MAX_SIZE more segments. The pool is full so they get garbage collected.
    before = pool.latestCounters();
    recycleSegments(pool, segments);
    after = pool.latestCounters();

    assertEquals("recycle: outstanding", 0, after.outstandingByteCount());
    assertEquals("recycle: deallocated", AllocatingPool.MAX_SIZE, deallocated(before, after));
    // Move the just-recycled frames to the freelist.
    pool.task.run();

    // Take MAX_SIZE segments to drain the pool.
    before = pool.latestCounters();
    segments.addAll(takeSegments(pool, AllocatingPool.MAX_SIZE));
    after = pool.latestCounters();

    assertEquals("drain: outstanding",
                 AllocatingPool.MAX_SIZE,
                 after.outstandingByteCount());
    assertEquals("drain: used", 0, after.usedByteCount());

    // Take MAX_SIZE more segments. The pool is drained so these will need to be allocated.
    before = pool.latestCounters();
    segments.addAll(takeSegments(pool, AllocatingPool.MAX_SIZE));
    after = pool.latestCounters();

    assertEquals("drain: allocated",
                 before.allocatedByteCount() + AllocatingPool.MAX_SIZE,
                 after.allocatedByteCount());
    assertEquals("drain: outstanding",
                 2 * AllocatingPool.MAX_SIZE,
                 after.outstandingByteCount());
    assertEquals("drain: used", 0, after.usedByteCount());
  }

  Collection<Segment> takeSegments(AllocatingPool pool, long bytes) {
    long remaining = bytes;
    Collection<Segment> segments = new ArrayList<>();
    while (remaining > 0) {
      segments.add(pool.take());
      remaining -= Segment.SIZE;
    }
    return segments;
  }

  long recycleSegments(AllocatingPool pool, Collection<Segment> segments) {
    long free = 0;
    for (Segment segment : segments) {
      pool.recycle(segment);
      free += Segment.SIZE;
    }
    segments.clear();
    return free;
  }

  long deallocated(PoolMetrics before, PoolMetrics after) {
    long intake = before.outstandingByteCount() - after.outstandingByteCount();
    long growth = after.usedByteCount() - before.usedByteCount();
    assertThat("growth <= intake", growth, lessThanOrEqualTo(intake));
    return intake - growth;
  }
}
