package okio.pool;

/**
* The default immutable implementation of the pool metrics API.
*/
class ImmutablePoolMetrics extends PoolMetrics {

  private final long usedByteCount;
  private final long allocatedByteCount;
  private final long outstandingByteCount;
  private final long totalAllocationCount;
  private final long totalTakeCount;

  private final long totalRecycleCount;

  ImmutablePoolMetrics(long usedByteCount,
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

  @Override public long usedByteCount() {
    return usedByteCount;
  }

  @Override public long allocatedByteCount() {
    return allocatedByteCount;
  }

  @Override public long outstandingByteCount() {
    return outstandingByteCount;
  }

  @Override public long totalAllocationCount() {
    return totalAllocationCount;
  }

  @Override public long totalTakeCount() {
    return totalTakeCount;
  }

  @Override public long totalRecycleCount() {
    return totalRecycleCount;
  }

}
