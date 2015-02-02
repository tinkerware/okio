package okio.pool;

import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class RecorderSetTest {

  static final int MAX_ITERATIONS = 10000;
  static final int MAX_TASKS = Runtime.getRuntime().availableProcessors() - 1;

  /*
   * By consistency, we mean that a reader will observe all changes to the
   * metrics atomically. Writers are spawned as individual threads and the
   * main thread acts as the single reader.
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
  public void snapshotsAreConsistent() throws InterruptedException {
    final Phaser loop = new Phaser() {
      @Override protected boolean onAdvance(int phase, int registeredParties) {
        return phase >= MAX_ITERATIONS || registeredParties == 0;
      }
    };
    final AtomicInteger countdownToGo = new AtomicInteger(MAX_TASKS + 1);

    class RecordMetrics implements Runnable {

      final MetricsDriver driver;

      RecordMetrics(MetricsRecorder recorder) {
        this.driver = new MetricsDriver(recorder);
      }

      @Override public void run() {
        while (!loop.isTerminated()) {
          loop.arriveAndAwaitAdvance();
          while (countdownToGo.get() > 0) {
            // We spin while other tasks are being unparked so that all
            // tasks can start recording as concurrently as we can make it.
          }
          driver.applyAlternate(1);
        }
      }
    }

    RecorderSet recorders = new RecorderSet();
    RecordMetrics[] tasks = new RecordMetrics[MAX_TASKS];
    loop.bulkRegister(tasks.length);
    for (int i = 0; i < tasks.length; i++) {
      tasks[i] = new RecordMetrics(recorders.singleWriterRecorder());
      Thread thread = new Thread(tasks[i], "Record Metrics - " + (i + 1));
      thread.start();
    }

    do {
      int phase = loop.register();
      while (loop.getUnarrivedParties() > 1) {
        // We spin for a little bit while tasks stop at sync point.
      }

      PoolMetrics baseline = recorders.aggregate();

      long baselineTakes = 0;
      long baselineRecycles = 0;
      for (RecordMetrics task : tasks) {
        baselineTakes += task.driver.takeCount;
        baselineRecycles += task.driver.recycleCount;
      }

      // Order is important here; we set the countdown
      // for the race before we let the writers observe
      // its value.
      countdownToGo.set(MAX_TASKS + 1);
      if (loop.arriveAndDeregister() < 0) {
        // We may be arriving after the phaser terminated the loop
        // once it went over the maximum iteration count; bail out
        // before hitting the busy spin in that case; there's no one
        // at the other end, which would cause an endless loop.
        break;
      }
      // The busy wait aligns the writer and reader threads as
      // closely as we can make it so that we get a good race.
      countdownToGo.getAndDecrement();
      while (countdownToGo.get() > 0) {
        // ready, set, go!
      }

      PoolMetrics current = recorders.aggregate();

      MetricsDriver delta = new MetricsDriver(MetricsRecorder.singleWriterRecorder(), baseline);
      delta.applyUse(current.totalTakeCount() - baselineTakes);
      delta.applyRecycle(current.totalRecycleCount() - baselineRecycles);

      assertEquals("baseline + delta: " + phase, current, delta.recorder.snapshot());
    }
    while (!loop.isTerminated());

  }

  private static class MetricsDriver {

    private MetricsRecorder recorder;
    private long takeCount;
    private long recycleCount;
    private boolean recycle;

    MetricsDriver(MetricsRecorder recorder) {
      this(recorder, PoolMetrics.zero());
    }

    MetricsDriver(MetricsRecorder recorder, PoolMetrics initial) {
      this.recorder = recorder;
      if (!initial.equals(PoolMetrics.zero())) {
        recorder.reset(initial);
      }
    }

    void applyAlternate(int delta) {
      for (int i = 0; i < delta; i++) {
        if (recycle) {
          recorder.recordRecycle(1, true);
          recycleCount++;
        }
        else {
          recorder.recordUse(1, true);
          takeCount++;
        }
        recycle ^= true;
      }
    }

    void applyUse(long delta) {
      for (int i = 0; i < delta; i++) {
        recorder.recordUse(1, true);
      }
      takeCount += delta;
    }

    void applyRecycle(long delta) {
      for (int i = 0; i < delta; i++) {
        recorder.recordRecycle(1, true);
      }
      recycleCount += delta;
    }
  }
}
