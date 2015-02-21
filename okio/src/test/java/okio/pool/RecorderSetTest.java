package okio.pool;

import java.util.Collection;
import java.util.Random;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class RecorderSetTest {

  static final int MAX_ITERATIONS = 5000;
  static final int MAX_TASKS = Runtime.getRuntime().availableProcessors() - 1;

  /*
   * By atomicity, we mean that a reader will observe all changes to the
   * metrics completely or not at all. Writers are spawned as individual
   * threads and the main thread acts as the single reader.
   *
   * The main body of the test executes `MAX_ITERATIONS` times. At each iteration,
   * we bring the writers to a sync point. At each such point, we capture a
   * baseline snapshot and record "witness" counters with which we can later
   * reproduce the actions of all writers. Afterwards, we bring the writers
   * out of the sync point carefully so that they race against the reader
   * thread while it captures a second, final snapshot.
   *
   * Given the final snapshot and the witness counters, we now (re)construct
   * the delta between the baseline and the final snapshot and verify that it
   * matches the observed difference between the final and baseline snapshots.
   *
   * This test is not deterministic; it may observe false negatives (though
   * not false positives).
   */

  @Test(timeout = 30000)
  public void snapshotsAreAtomic() throws InterruptedException {
    final Phaser loop = new Phaser() {
      @Override protected boolean onAdvance(int phase, int registeredParties) {
        return phase >= MAX_ITERATIONS || registeredParties == 0;
      }
    };

    final AtomicBoolean ready = new AtomicBoolean();

    class RecordMetrics implements Runnable {
      final Random random = new Random();
      volatile MetricsDelta delta;

      @Override public void run() {
        while (!loop.isTerminated()) {
          // We wait for other other recorders to arrive at
          // sync point, then release simultaneously.
          loop.arriveAndAwaitAdvance();
          performIteration(1000 * 1000);
        }
      }

      void performIteration(int count) {
        MetricsDelta delta = this.delta;
        for (int i = 0; i < count && !ready.get(); i++) {
          if (random.nextBoolean()) {
            delta.applyUse(1);
          }
          else {
            delta.applyRecycle(1);
          }
        }
      }
    }

    final RecordMetrics[] tasks = new RecordMetrics[MAX_TASKS];
    loop.bulkRegister(tasks.length + 1);
    for (int i = 0; i < tasks.length; i++) {
      tasks[i] = new RecordMetrics();
      Thread thread = new Thread(tasks[i], "Record Metrics - " + (i + 1));
      thread.start();
    }

    PoolMetrics baseline = PoolMetrics.zero();
    do {

      // Ready...
      ready.set(true);
      while (loop.getUnarrivedParties() > 1) {
        // We spin for a little bit while tasks stop at sync point.
      }
      int phase = loop.getPhase();

      // Set...
      final RecorderSet recorders = new RecorderSet();
      long finalTakes = 0;
      long finalRecycles = 0;
      for (RecordMetrics task : tasks) {
        if (task.delta != null) {
          // We know that delta counts are most recent since
          // the workers arrive at sync point (after their last
          // write to recorders' counters) before we observe them
          // doing so in the busy spin above.
          finalTakes += task.delta.takeCount;
          finalRecycles += task.delta.recycleCount;
        }
        task.delta = new MetricsDelta(recorders.singleWriterRecorder());
      }

      MetricsDelta updated = new MetricsDelta();
      updated.applyUse(finalTakes - baseline.totalTakeCount());
      updated.applyRecycle(finalRecycles - baseline.totalRecycleCount());
      PoolMetrics actual = baseline.merge(updated.recorder.snapshot());

      MetricsDelta total = new MetricsDelta();
      total.applyUse(finalTakes);
      total.applyRecycle(finalRecycles);
      PoolMetrics expected = total.recorder.snapshot();

      assertEquals("total <> (last + delta): iteration " + phase, expected, actual);

      // Go!
      ready.set(false);
      loop.arrive();
      Thread.yield();

      baseline = recorders.aggregate();
    }
    while (!loop.isTerminated());

  }

  private static class MetricsDelta {
    private final MetricsRecorder recorder;
    private long takeCount;
    private long recycleCount;

    MetricsDelta() {
      this(MetricsRecorder.singleWriterRecorder());
    }

    MetricsDelta(MetricsRecorder recorder) {
      this.recorder = recorder;
    }

    void applyUse(long delta) {
      assertTrue("use delta negative: " + delta, delta >= 0);
      for (int i = 0; i < delta; i++) {
        recorder.recordUse(1, true);
      }
      takeCount += delta;
    }

    void applyRecycle(long delta) {
      assertTrue("recycle delta negative: " + delta, delta >= 0);
      for (int i = 0; i < delta; i++) {
        recorder.recordRecycle(1, true);
      }
      recycleCount += delta;
    }
  }
}
