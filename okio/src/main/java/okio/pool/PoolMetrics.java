package okio.pool;

/**
 * An immutable snapshot of metrics on segment pool usage.
 */
public class PoolMetrics {

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

  public long usedByteCount() {
    return usedByteCount;
  }

  public long allocatedByteCount() {
    return allocatedByteCount;
  }

  public long outstandingByteCount() {
    return outstandingByteCount;
  }

  public long totalAllocationCount() {
    return totalAllocationCount;
  }

  public long totalTakeCount() {
    return totalTakeCount;
  }

  public long totalRecycleCount() {
    return totalRecycleCount;
  }

  public static class Recorder {

    private final LongAdder usedByteCount = new LongAdder();
    private final LongAdder allocatedByteCount = new LongAdder();
    private final LongAdder outstandingByteCount = new LongAdder();
    private final LongAdder totalAllocationCount = new LongAdder();
    private final LongAdder totalTakeCount = new LongAdder();
    private final LongAdder totalRecycleCount = new LongAdder();

    public PoolMetrics snapshot() {
      return new PoolMetrics(usedByteCount.sum(),
                             allocatedByteCount.sum(),
                             outstandingByteCount.sum(),
                             totalAllocationCount.sum(),
                             totalTakeCount.sum(),
                             totalRecycleCount.sum());
    }

    public void recordUse(int segmentSize, boolean allocated) {
      checkSize(segmentSize);

      if (allocated) {
        allocatedByteCount.add(segmentSize);
        totalAllocationCount.increment();
      }
      else {
        usedByteCount.add(-segmentSize);
      }
      outstandingByteCount.add(segmentSize);
      totalTakeCount.increment();
    }

    public void recordRecycle(int segmentSize, boolean deallocated) {
      checkSize(segmentSize);

      if (!deallocated) {
        usedByteCount.add(segmentSize);
      }
      outstandingByteCount.add(-segmentSize);
      totalRecycleCount.increment();
    }

    public void recordShrink(int segmentSize) {
      checkSize(segmentSize);
      usedByteCount.add(-segmentSize);
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
