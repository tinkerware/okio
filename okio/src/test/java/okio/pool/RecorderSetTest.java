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
   * out of the safe point carefully so that they race against the reader
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

      final MetricsDriver driver;

      RecordMetrics(MetricsRecorder recorder) {
        this.driver = new MetricsDriver(recorder, ready);
      }

      @Override public void run() {
        while (!loop.isTerminated()) {
          // We wait for other other recorders to arrive at
          // sync point, then release simultaneously.
          loop.arriveAndAwaitAdvance();
          driver.performIteration(1000 * 1000);
        }
      }
    }

    final RecorderSet recorders = new RecorderSet();
    final RecordMetrics[] tasks = new RecordMetrics[MAX_TASKS];
    loop.bulkRegister(tasks.length + 1);
    for (int i = 0; i < tasks.length; i++) {
      tasks[i] = new RecordMetrics(recorders.singleWriterRecorder());
      Thread thread = new Thread(tasks[i], "Record Metrics - " + (i + 1));
      thread.start();
    }

    do {
      // Ready...
      ready.set(true);
      while (loop.getUnarrivedParties() > 1) {
        // We spin for a little bit while tasks stop at sync point.
      }


      int phase = loop.getPhase();
      PoolMetrics baseline = recorders.aggregate();
      // Set...
      long baselineTakes = 0;
      long baselineRecycles = 0;
      for (RecordMetrics task : tasks) {
        baselineTakes += task.driver.takeCount;
        baselineRecycles += task.driver.recycleCount;
      }

      // Go!
      ready.set(false);
      loop.arrive();
      Thread.yield();

      PoolMetrics current = recorders.aggregate();

      MetricsDriver updated = new MetricsDriver(MetricsRecorder.singleWriterRecorder(), baseline);
      updated.applyUse(current.totalTakeCount() - baselineTakes);
      updated.applyRecycle(current.totalRecycleCount() - baselineRecycles);

      assertEquals("baseline <> updated: iteration " + phase, current, updated.recorder.snapshot());
    }
    while (!loop.isTerminated());

  }

  private static class MetricsDriver {

    private final Random random = new Random();
    private final MetricsRecorder recorder;
    private final AtomicBoolean ready;
    private long takeCount;
    private long recycleCount;

    MetricsDriver(MetricsRecorder recorder, AtomicBoolean ready) {
      this(recorder, PoolMetrics.zero(), ready);
    }

    MetricsDriver(MetricsRecorder recorder, PoolMetrics initial) {
      this(recorder, initial, new AtomicBoolean());
    }

    MetricsDriver(MetricsRecorder recorder, PoolMetrics initial, AtomicBoolean ready) {
      this.recorder = recorder;
      this.ready = ready;
      if (!initial.equals(PoolMetrics.zero())) {
        recorder.reset(initial);
      }
    }

    void performIteration(int delta) {
      for (int i = 0; i < delta && !ready.get(); i++) {
        if (random.nextBoolean()) {
          recorder.recordRecycle(1, true);
          recycleCount++;
        }
        else {
          recorder.recordUse(1, true);
          takeCount++;
        }
      }
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
