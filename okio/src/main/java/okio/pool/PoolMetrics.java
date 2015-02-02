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
