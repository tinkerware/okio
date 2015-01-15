package okio;

/**
 * Provides access to common segment pools.
 */
final class SegmentPools {

  private SegmentPools() { /* not allowed */ }

  static SegmentPool linkedPool() {
    return LinkedSegmentPool.INSTANCE;
  }

  static SegmentPool concurrentPool() {
    return ConcurrentSegmentPool.INSTANCE;
  }

  private static final SegmentPool bestPool;

  static {
    long heapSizeThreshold = 1024 * 1024 * 1024; // 1 GiB
    long cpuCountThreshold = 4;

    int cpuCount = Runtime.getRuntime().availableProcessors();
    long maxHeap = Runtime.getRuntime().maxMemory();
    boolean concurrentOverride = Boolean.getBoolean("okio.pool.concurrent");

    boolean concurrentEnv = concurrentOverride ||
                            cpuCount >= cpuCountThreshold ||
                            maxHeap >= heapSizeThreshold;

    bestPool = concurrentEnv ? concurrentPool() : linkedPool();
  }

  /**
   * Returns a shared buffer pool most suitable for use in this runtime.
   */
  static SegmentPool commonPool() {
    return bestPool;
  }

}
