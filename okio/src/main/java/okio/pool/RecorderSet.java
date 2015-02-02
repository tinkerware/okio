package okio.pool;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Manages and aggregates a set of recorders while providing atomicity and
 * thread safety guarantees.
 * <p>
 * Metrics recorders that are part of this set must be created with one of
 * {@link #singleWriterRecorder()} or {@link #multiWriterRecorder} methods.
 * They do not support the {@link  MetricsRecorder#reset()} method.
 * <p>
 * This set guarantees that the {@link #aggregate()} and the {@link MetricsRecorder#snapshot()}
 * methods never block the {@code record} methods of individual recorders and
 * that their results atomically include or exclude updates to any of the
 * counters in this list. On platforms that support atomic fetch & add
 * (such as Java 8) recorder updates are guaranteed to be wait-free (i.e. all
 * recorders make progress); otherwise they are lock-free (i.e. at least one
 * recorder makes progress).
 *
 */
public class RecorderSet {

  class AggregateRecorder extends MetricsRecorder {

    private volatile MetricsRecorder active;
    private MetricsRecorder inactive;
    private PoolMetrics aggregated = PoolMetrics.zero();

    AggregateRecorder(RecorderSource source) {
      this.active = source.get();
      this.inactive = source.get();
    }

    /**
     * Resets the inactive recorder and swaps it with the active recorder. Must be called when
     * holding the reader lock.
     */
    void atomicSwapAndReset() {
      final MetricsRecorder temp = inactive;
      temp.reset();
      inactive = active;
      active = temp;
    }

    /**
     * Aggregates the active recorder's counters with the accumulated values. Must be called when
     * holding the reader lock.
     */
    PoolMetrics aggregate() {
      aggregated = aggregated.merge(inactive.snapshot());
      return aggregated;
    }

    @Override public PoolMetrics snapshot() {
      recordingPhaser.readerLock();
      try {
        atomicSwapAndReset();
        recordingPhaser.flipPhase();
        return aggregate();
      }
      finally {
        recordingPhaser.readerUnlock();
      }
    }

    @Override public void reset() {
      throw new UnsupportedOperationException("reset");
    }

    @Override void reset(PoolMetrics initialState) {
      throw new UnsupportedOperationException("reset/initialState");
    }

    @Override public void recordUse(int segmentSize, boolean allocated) {
      long phase = recordingPhaser.writerCriticalSectionEnter();
      try {
        active.recordUse(segmentSize, allocated);
      }
      finally {
        recordingPhaser.writerCriticalSectionExit(phase);
      }
    }

    @Override public void recordRecycle(int segmentSize, boolean deallocated) {
      long phase = recordingPhaser.writerCriticalSectionEnter();
      try {
        active.recordRecycle(segmentSize, deallocated);
      }
      finally {
        recordingPhaser.writerCriticalSectionExit(phase);
      }
    }

    @Override public void recordTrim(int segmentSize) {
      long phase = recordingPhaser.writerCriticalSectionEnter();
      try {
        active.recordTrim(segmentSize);
      }
      finally {
        recordingPhaser.writerCriticalSectionExit(phase);
      }
    }

  }

  interface RecorderSource {

    MetricsRecorder get();
  }

  private final Collection<AggregateRecorder> recorders = new CopyOnWriteArrayList<>();
  private final WriterReaderPhaser recordingPhaser = new WriterReaderPhaser();

  public MetricsRecorder singleWriterRecorder() {
    return createRecorder(new RecorderSource() {
      @Override public MetricsRecorder get() {
        return MetricsRecorder.singleWriterRecorder();
      }
    });
  }

  public MetricsRecorder multiWriterRecorder() {
    return createRecorder(new RecorderSource() {
      @Override public MetricsRecorder get() {
        return MetricsRecorder.multiWriterRecorder();
      }
    });
  }

  private MetricsRecorder createRecorder(RecorderSource source) {
    AggregateRecorder recorder = new AggregateRecorder(source);
    recorders.add(recorder);
    return recorder;
  }

  public PoolMetrics aggregate() {
    Collection<PoolMetrics> aggregates = new ArrayList<>(recorders.size());

    recordingPhaser.readerLock();
    try {
      for (AggregateRecorder recorder : recorders) {
        recorder.atomicSwapAndReset();
      }
      flipPhase(recordingPhaser);
      for (AggregateRecorder recorder : recorders) {
        aggregates.add(recorder.aggregate());
      }
    }
    finally {
      recordingPhaser.readerUnlock();
    }

    return PoolMetrics.merge(aggregates);
  }

  void flipPhase(WriterReaderPhaser phaser) {
    phaser.flipPhase();
  }
}
