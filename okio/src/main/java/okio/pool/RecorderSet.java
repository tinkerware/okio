package okio.pool;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
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
public class RecorderSet implements Iterable<MetricsRecorder> {

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
      snapshotPhaser.readerLock();
      try {
        atomicSwapAndReset();
        flipPhase();
        return aggregate();
      }
      finally {
        snapshotPhaser.readerUnlock();
      }
    }

    @Override public void reset() {
      throw new UnsupportedOperationException("reset");
    }

    @Override void reset(PoolMetrics initialState) {
      throw new UnsupportedOperationException("reset/initialState");
    }

    @Override public void recordUse(int segmentSize, boolean allocated) {
      long phase = snapshotPhaser.writerCriticalSectionEnter();
      try {
        active.recordUse(segmentSize, allocated);
      }
      finally {
        snapshotPhaser.writerCriticalSectionExit(phase);
      }
    }

    @Override public void recordRecycle(int segmentSize, boolean deallocated) {
      long phase = snapshotPhaser.writerCriticalSectionEnter();
      try {
        active.recordRecycle(segmentSize, deallocated);
      }
      finally {
        snapshotPhaser.writerCriticalSectionExit(phase);
      }
    }

    @Override public void recordTrim(int segmentSize) {
      long phase = snapshotPhaser.writerCriticalSectionEnter();
      try {
        active.recordTrim(segmentSize);
      }
      finally {
        snapshotPhaser.writerCriticalSectionExit(phase);
      }
    }

  }

  interface RecorderSource {

    MetricsRecorder get();
  }

  private final Collection<AggregateRecorder> recorders = new CopyOnWriteArrayList<>();
  private final WriterReaderPhaser snapshotPhaser = new WriterReaderPhaser();

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

    snapshotPhaser.readerLock();
    try {
      swapAndCollectInto(recorders, aggregates);
    }
    finally {
      snapshotPhaser.readerUnlock();
    }

    return PoolMetrics.merge(aggregates);
  }

  public PoolMetrics remove(Collection<MetricsRecorder> recordersToRemove) {
    Collection<AggregateRecorder> toRemove = checkedDowncast(recordersToRemove,
                                                             AggregateRecorder.class);
    Collection<PoolMetrics> collected = new ArrayList<>(toRemove.size());
    snapshotPhaser.readerLock();
    try {
      if (recorders.removeAll(toRemove)) {
        swapAndCollectInto(toRemove, collected);
      }
    } finally {
      snapshotPhaser.readerUnlock();
    }
    return PoolMetrics.merge(collected);
  }

  void swapAndCollectInto(Iterable<AggregateRecorder> recorders,
                                  Collection<PoolMetrics> aggregates) {
    for (AggregateRecorder recorder : recorders) {
      recorder.atomicSwapAndReset();
    }
    flipPhase();
    for (AggregateRecorder recorder : recorders) {
      aggregates.add(recorder.aggregate());
    }
  }

  void flipPhase() {
    snapshotPhaser.flipPhase();
  }

  @SuppressWarnings("unchecked")
  private <V, U extends V> Collection<U> checkedDowncast(Collection<V> toConvert,
                                                         Class<U> convertedType) {
    Collection<V> converted =
        (Collection<V>) Collections.checkedCollection(new ArrayList<U>(), convertedType);
    converted.addAll(toConvert);
    return (Collection<U>) converted;
  }

  public List<MetricsRecorder> toList() {
    return new ArrayList<MetricsRecorder>(recorders);
  }

  @Override public Iterator<MetricsRecorder> iterator() {
    return capture(recorders.iterator());
  }

  @SuppressWarnings("unchecked")
  private <T extends MetricsRecorder> Iterator<MetricsRecorder> capture(Iterator<T> iterator) {
    return (Iterator<MetricsRecorder>) iterator;
  }

}
