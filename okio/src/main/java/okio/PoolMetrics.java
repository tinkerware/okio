package okio;

/**
 * An immutable snapshot of metrics on segment pool usage.
 */
class PoolMetrics {

  private final long usedByteCount;
  private final long allocatedByteCount;
  private final long outstandingByteCount;
  private final long totalAllocationCount;
  private final long totalTakeCount;
  private final long totalRecycleCount;

  private PoolMetrics(long usedByteCount,
                      long allocatedByteCount,
                      long outstandingByteCount,
                      long totalAllocationCount,
                      long totalTakeCount,
                      long totalRecycleCount) {

    this.usedByteCount = usedByteCount;
    this.allocatedByteCount = allocatedByteCount;
    this.outstandingByteCount = outstandingByteCount;
    this.totalAllocationCount = totalAllocationCount;
    this.totalTakeCount = totalTakeCount;
    this.totalRecycleCount = totalRecycleCount;
  }

  long usedByteCount() {
    return usedByteCount;
  }

  long allocatedByteCount() {
    return allocatedByteCount;
  }

  long outstandingByteCount() {
    return outstandingByteCount;
  }

  long totalAllocationCount() {
    return totalAllocationCount;
  }

  long totalTakeCount() {
    return totalTakeCount;
  }

  long totalRecycleCount() {
    return totalRecycleCount;
  }

  static class Recorder {

    final LongAdder usedByteCount = new LongAdder();
    final LongAdder allocatedByteCount = new LongAdder();
    final LongAdder outstandingByteCount = new LongAdder();
    final LongAdder totalAllocationCount = new LongAdder();
    final LongAdder totalTakeCount = new LongAdder();
    final LongAdder totalRecycleCount = new LongAdder();

    PoolMetrics snapshot() {
      return new PoolMetrics(usedByteCount.sum(),
                             allocatedByteCount.sum(),
                             outstandingByteCount.sum(),
                             totalAllocationCount.sum(),
                             totalTakeCount.sum(),
                             totalRecycleCount.sum());
    }

    void recordSegmentTake(int size, boolean allocated) {
      checkSize(size);

      if (allocated) {
        allocatedByteCount.add(size);
        totalAllocationCount.increment();
      }
      else {
        usedByteCount.add(-size);
      }
      outstandingByteCount.add(size);
      totalTakeCount.increment();
    }

    void recordSegmentRecycle(int size, boolean deallocated) {
      checkSize(size);

      if (!deallocated) {
        usedByteCount.add(size);
      }
      outstandingByteCount.add(-size);
      totalRecycleCount.increment();
    }

    private void checkSize(int size) {
      if (size < 0) throw new IllegalArgumentException("size < 0");
    }

  }

  @Override public boolean equals(Object obj) {
    if (obj == this) return true;
    if (!(obj instanceof PoolMetrics)) return false;

    PoolMetrics other = (PoolMetrics) obj;
    return usedByteCount == other.usedByteCount &&
           allocatedByteCount == other.allocatedByteCount &&
           outstandingByteCount == other.outstandingByteCount &&
           totalAllocationCount == other.totalAllocationCount &&
           totalTakeCount == other.totalTakeCount &&
           totalRecycleCount == other.totalRecycleCount;
  }

  @Override public int hashCode() {
    int result = 17;
    result = 31 * result + (int) (usedByteCount ^ (usedByteCount >>> 32));
    result = 31 * result + (int) (allocatedByteCount ^ (allocatedByteCount >>> 32));
    result = 31 * result + (int) (outstandingByteCount ^ (outstandingByteCount >>> 32));
    result = 31 * result + (int) (totalAllocationCount ^ (totalAllocationCount >>> 32));
    result = 31 * result + (int) (totalTakeCount ^ (totalTakeCount >>> 32));
    result = 31 * result + (int) (totalRecycleCount ^ (totalRecycleCount >>> 32));
    return result;
  }

  @Override public String toString() {
    return String.format("PoolMetrics[usedBytes=%d allocBytes=%d outBytes=%d totalAllocs=%d totalTakes=%d totalRecycles=%d",
                         usedByteCount,
                         allocatedByteCount,
                         outstandingByteCount,
                         totalAllocationCount,
                         totalTakeCount,
                         totalRecycleCount);
  }
}
