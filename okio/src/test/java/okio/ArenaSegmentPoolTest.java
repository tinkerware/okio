package okio;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Phaser;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import okio.pool.PoolMetrics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ArenaSegmentPoolTest {

  private static class NonCleaningPool extends ArenaSegmentPool {

    CleanTask task;

    @Override CleanTask createCleanTask() {
      task = new CleanTask() {
        @Override public ScheduledFuture<?> schedule() {
          return null;
        }
      };
      return task;
    }
  }

  @Test public void noLeaksAfterThreadDeath() throws InterruptedException {

    final NonCleaningPool pool = new NonCleaningPool();

    Thread t = new Thread(new Runnable() {
      @Override public void run() {
        try {
          pool.recycle(pool.take());
        }
        catch (Exception e) {
          fail(e.getMessage());
        }
      }
    });

    t.start();
    t.join();

    // We should still have the arena reference after its thread dies.
    assertEquals(1, pool.arenaCount());
    // Let's give GC a chance to process weak references.
    System.gc();
    // Now we can expunge our stale entries.
    pool.task.run();
    // We should have no references to the arena of the dead thread.
    assertEquals(0, pool.arenaCount());

  }

  @Test public void arenasStealSegments() throws Throwable {
    final NonCleaningPool pool = new NonCleaningPool();

    /*
     * The phaser coordinates the main thread and the allocating "t1" thread
     * (the two parties). A party signals the other that it is ready to advance
     * to the next phase by arriving. When all parties arrive at a phase, the
     * phaser advances to the next phase. The phase terminates if any party
     * forces termination or all parties deregister from the phaser.
     *
     * The allocating thread communicates any errors or assertion failures
     * back to the main thread by forcing termination of the phaser and sending
     * the throwable with the `error` synchronous queue.
     */
    final Phaser nextPhase = new Phaser(2);
    final SynchronousQueue<Throwable> error = new SynchronousQueue<>();

    Thread t1 = new Thread(new Runnable() {
      @Override public void run() {
        try {
          ArenaSegmentPool.Arena arena = pool.arena.get();

          // Allocate and recycle a full arena worth of segments.
          recycleSegments(pool, takeSegments(pool, AllocatingPool.MAX_SIZE));

          int freeSegments = arena.segmentCount();
          PoolMetrics metrics = pool.metrics();
          assertEquals("recycles", freeSegments, metrics.totalRecycleCount());

          // Now, we signal the main thread to start stealing segments.
          nextPhase.arriveAndAwaitAdvance();
          // We wait (for the last time) until there's exactly one stolen segment.
          nextPhase.awaitAdvance(nextPhase.arriveAndDeregister());
          assertEquals("free segments", freeSegments - 1, arena.segmentCount());
        }
        catch (Throwable e) {
          nextPhase.forceTermination();
          try {
            error.put(e);
          }
          catch (InterruptedException ie) {
            throw new RuntimeException(ie);
          }
        }
      }
    });

    t1.start();
    // We wait until t1 signals that there are free segments we can steal.
    nextPhase.arriveAndAwaitAdvance();
    if (nextPhase.isTerminated()) {
      throw error.take();
    }

    int takesUntilSteal = 0;
    PoolMetrics poolMetrics;
    do {
      takesUntilSteal++;
      // We discard the segment immediately rather than recycle
      // so that this thread's arena must always attempt to steal.
      pool.take();
      poolMetrics = pool.metrics();
    }
    while (poolMetrics.totalAllocationCount() == poolMetrics.totalTakeCount());

    // We signal that we stole one segment from t1's arena.
    // We do not deregister so that we can check below if phaser was forced
    // to terminate to indicate an error.
    nextPhase.arrive();

    // Don't forget to wait for t1's catch block in case we have an error.
    t1.join();
    if (nextPhase.isTerminated()) {
      throw error.take();
    }

    assertEquals("Pool size inconsistent with takes until steal: " + takesUntilSteal,
                 poolMetrics.allocatedByteCount(),
                 poolMetrics.usedByteCount() + takesUntilSteal * Segment.SIZE);
  }

  Collection<Segment> takeSegments(AllocatingPool pool, long bytes) {
    long remaining = bytes;
    List<Segment> segments = new ArrayList<>();
    while (remaining > 0) {
      segments.add(pool.take());
      remaining -= Segment.SIZE;
    }
    return segments;
  }

  long recycleSegments(AllocatingPool pool,  Collection<Segment> segments) {
    long free = 0;
    for (Segment segment : segments) {
      pool.recycle(segment);
      free += Segment.SIZE;
    }
    return free;
  }
}
