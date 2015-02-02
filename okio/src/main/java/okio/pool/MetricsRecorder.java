package okio.pool;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

/**
* Records pool metrics and creates snapshots over a time interval.
*/
public abstract class MetricsRecorder {

  public static MetricsRecorder singleWriterRecorder() {
    return new SingleWriterRecorder();
  }

  public static MetricsRecorder multiWriterRecorder() {
    return new MultiWriterRecorder();
  }

  public abstract PoolMetrics snapshot();

  public void reset() {
    reset(PoolMetrics.zero());
  }

  public abstract void recordUse(int segmentSize, boolean allocated);

  public abstract void recordRecycle(int segmentSize, boolean deallocated);

  public abstract void recordTrim(int segmentSize);

  abstract void reset(PoolMetrics initialState);

  private static class SingleWriterRecorder extends MetricsRecorder {

    private long usedByteCount = 0;
    private long allocatedByteCount = 0;
    private long outstandingByteCount = 0;
    private long totalAllocationCount = 0;
    private long totalTakeCount = 0;
    private long totalRecycleCount = 0;

    @Override public PoolMetrics snapshot() {
      return new ImmutablePoolMetrics(usedByteCount,
                                      allocatedByteCount,
                                      outstandingByteCount,
                                      totalAllocationCount,
                                      totalTakeCount,
                                      totalRecycleCount);
    }

    @Override void reset(PoolMetrics initialState) {
      usedByteCount = initialState.usedByteCount();
      allocatedByteCount = initialState.allocatedByteCount();
      outstandingByteCount = initialState.outstandingByteCount();
      totalAllocationCount = initialState.totalAllocationCount();
      totalTakeCount = initialState.totalTakeCount();
      totalRecycleCount = initialState.totalRecycleCount();
    }

    @Override public void recordUse(int segmentSize, boolean allocated) {
      if (allocated) {
        allocatedByteCount += segmentSize;
        totalAllocationCount++;
      }
      else {
        usedByteCount -= segmentSize;
      }
      outstandingByteCount += segmentSize;
      totalTakeCount++;
    }

    @Override public void recordRecycle(int segmentSize, boolean deallocated) {
      if (!deallocated) {
        usedByteCount += segmentSize;
      }
      outstandingByteCount -= segmentSize;
      totalRecycleCount++;
    }

    @Override public void recordTrim(int segmentSize) {
      usedByteCount -= segmentSize;
    }

  }

  @SuppressWarnings("UnusedDeclaration")
  private static class MultiWriterRecorder extends MetricsRecorder {

    private volatile long usedByteCount = 0;
    private volatile long allocatedByteCount = 0;
    private volatile long outstandingByteCount = 0;
    private volatile long totalAllocationCount = 0;
    private volatile long totalTakeCount = 0;
    private volatile long totalRecycleCount = 0;

    @Override public PoolMetrics snapshot() {
      return new ImmutablePoolMetrics(usedByteCount,
                                      allocatedByteCount,
                                      outstandingByteCount,
                                      totalAllocationCount,
                                      totalTakeCount,
                                      totalRecycleCount);
    }

    @Override public void reset(PoolMetrics initialState) {
      if (initialState == null) throw new NullPointerException("initialState");

      atomicUsed.set(this, initialState.usedByteCount());
      atomicAllocated.set(this, initialState.allocatedByteCount());
      atomicOutstanding.set(this, initialState.outstandingByteCount());
      atomicAllocations.set(this, initialState.totalAllocationCount());
      atomicTakes.set(this, initialState.totalTakeCount());
      atomicRecycles.set(this, initialState.totalRecycleCount());
    }

    @Override public void recordUse(int segmentSize, boolean allocated) {
      if (allocated) {
        atomicAllocated.getAndAdd(this, segmentSize);
        atomicAllocations.getAndIncrement(this);
      }
      else {
        atomicUsed.getAndAdd(this, -segmentSize);
      }
      atomicOutstanding.getAndAdd(this, segmentSize);
      atomicTakes.getAndIncrement(this);
    }

    @Override public void recordRecycle(int segmentSize, boolean deallocated) {
      if (!deallocated) {
        atomicUsed.getAndAdd(this, segmentSize);
      }
      atomicOutstanding.getAndAdd(this, -segmentSize);
      atomicRecycles.getAndIncrement(this);
    }

    @Override public void recordTrim(int segmentSize) {
      atomicUsed.getAndAdd(this, -segmentSize);
    }

    private static final AtomicLongFieldUpdater<MultiWriterRecorder> atomicUsed =
        AtomicLongFieldUpdater.newUpdater(MultiWriterRecorder.class, "usedByteCount");
    private static final AtomicLongFieldUpdater<MultiWriterRecorder> atomicAllocated =
        AtomicLongFieldUpdater.newUpdater(MultiWriterRecorder.class, "allocatedByteCount");
    private static final AtomicLongFieldUpdater<MultiWriterRecorder> atomicOutstanding =
        AtomicLongFieldUpdater.newUpdater(MultiWriterRecorder.class, "outstandingByteCount");
    private static final AtomicLongFieldUpdater<MultiWriterRecorder> atomicAllocations =
        AtomicLongFieldUpdater.newUpdater(MultiWriterRecorder.class, "totalAllocationCount");
    private static final AtomicLongFieldUpdater<MultiWriterRecorder> atomicTakes =
        AtomicLongFieldUpdater.newUpdater(MultiWriterRecorder.class, "totalTakeCount");
    private static final AtomicLongFieldUpdater<MultiWriterRecorder> atomicRecycles =
        AtomicLongFieldUpdater.newUpdater(MultiWriterRecorder.class, "totalRecycleCount");
  }
}
