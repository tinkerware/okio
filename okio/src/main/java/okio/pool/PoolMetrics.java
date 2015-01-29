package okio.pool;

/**
 * An immutable snapshot of metrics on segment pool usage.
 */
public class PoolMetrics {

  public static PoolMetrics zero() {
    return new PoolMetrics(0, 0, 0, 0, 0, 0);
  }

  public static PoolMetrics merge(Iterable<PoolMetrics> sequence) {
    PoolMetrics result = PoolMetrics.zero();
    for (PoolMetrics metrics : sequence) {
      result = result.merge(metrics);
    }
    return result;
  }

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

  public PoolMetrics merge(PoolMetrics other) {
    if (other == null) throw new NullPointerException("other");
    return new PoolMetrics(usedByteCount + other.usedByteCount,
                           allocatedByteCount + other.allocatedByteCount,
                           outstandingByteCount + other.outstandingByteCount,
                           totalAllocationCount + other.totalAllocationCount,
                           totalTakeCount + other.totalTakeCount,
                           totalRecycleCount + other.totalRecycleCount);
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
    return String.format("PoolMetrics[used=%d allocated=%d outstanding=%d allocations=%d takes=%d recycles=%d",
                         usedByteCount,
                         allocatedByteCount,
                         outstandingByteCount,
                         totalAllocationCount,
                         totalTakeCount,
                         totalRecycleCount);
  }

  public static class Recorder {

    private long usedByteCount = 0;
    private long allocatedByteCount = 0;
    private long outstandingByteCount = 0;
    private long totalAllocationCount = 0;
    private long totalTakeCount = 0;
    private long totalRecycleCount = 0;

    public PoolMetrics snapshot() {
      return new PoolMetrics(usedByteCount,
                             allocatedByteCount,
                             outstandingByteCount,
                             totalAllocationCount,
                             totalTakeCount,
                             totalRecycleCount);
    }

    public void recordUse(int segmentSize, boolean allocated) {
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

    public void recordRecycle(int segmentSize, boolean deallocated) {
      if (!deallocated) {
        usedByteCount += segmentSize;
      }
      outstandingByteCount -= segmentSize;
      totalRecycleCount++;
    }

    public void recordTrim(int segmentSize) {
      usedByteCount -= segmentSize;
    }

  }

}
