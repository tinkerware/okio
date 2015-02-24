package okio.pool;

/**
 * An immutable snapshot of metrics on segment pool usage.
 */
public abstract class PoolMetrics {

  public static PoolMetrics zero() {
    return new ImmutablePoolMetrics(0, 0, 0, 0, 0, 0);
  }

  public static PoolMetrics merge(Iterable<PoolMetrics> sequence) {
    PoolMetrics result = PoolMetrics.zero();
    for (PoolMetrics metrics : sequence) {
      result = result.merge(metrics);
    }
    return result;
  }

  public abstract long usedByteCount();

  public abstract long allocatedByteCount();

  public abstract long outstandingByteCount();

  public abstract long totalAllocationCount();

  public abstract long totalTakeCount();

  public abstract long totalRecycleCount();

  public PoolMetrics merge(PoolMetrics other) {
    if (other == null) throw new NullPointerException("other");
    return new ImmutablePoolMetrics(usedByteCount() + other.usedByteCount(),
                                    allocatedByteCount() + other.allocatedByteCount(),
                                    outstandingByteCount() + other.outstandingByteCount(),
                                    totalAllocationCount() + other.totalAllocationCount(),
                                    totalTakeCount() + other.totalTakeCount(),
                                    totalRecycleCount() + other.totalRecycleCount());
  }

  @Override public boolean equals(Object obj) {
    if (obj == this) return true;
    if (!(obj instanceof PoolMetrics)) return false;

    PoolMetrics other = (PoolMetrics) obj;
    return usedByteCount() == other.usedByteCount() &&
           allocatedByteCount() == other.allocatedByteCount() &&
           outstandingByteCount() == other.outstandingByteCount() &&
           totalAllocationCount() == other.totalAllocationCount() &&
           totalTakeCount() == other.totalTakeCount() &&
           totalRecycleCount() == other.totalRecycleCount();
  }

  @Override public int hashCode() {
    int result = 17;
    result = 31 * result + (int) (usedByteCount() ^ (usedByteCount() >>> 32));
    result = 31 * result + (int) (allocatedByteCount() ^ (allocatedByteCount() >>> 32));
    result = 31 * result + (int) (outstandingByteCount() ^ (outstandingByteCount() >>> 32));
    result = 31 * result + (int) (totalAllocationCount() ^ (totalAllocationCount() >>> 32));
    result = 31 * result + (int) (totalTakeCount() ^ (totalTakeCount() >>> 32));
    result = 31 * result + (int) (totalRecycleCount() ^ (totalRecycleCount() >>> 32));
    return result;
  }

  @Override public String toString() {
    return String.format("PoolMetrics{used=%d allocated=%d outstanding=%d allocations=%d takes=%d recycles=%d}",
                         usedByteCount(),
                         allocatedByteCount(),
                         outstandingByteCount(),
                         totalAllocationCount(),
                         totalTakeCount(),
                         totalRecycleCount());
  }
}
