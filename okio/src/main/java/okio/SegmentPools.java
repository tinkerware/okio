package okio;

import okio.pool.SegmentPool;

/**
 * Provides access to common segment pools.
 */
final class SegmentPools {

  private SegmentPools() { /* not allowed */ }

  private static SegmentPool linkedPool() {
    return LinkedSegmentPool.INSTANCE;
  }

  private static SegmentPool concurrentPool() {
    return ConcurrentSegmentPool.INSTANCE;
  }

  private static final SegmentPool bestPool;

  static {
    long heapSizeThreshold = 1024 * 1024 * 1024; // 1 GiB
    long cpuCountThreshold = 4;

    int cpuCount = Runtime.getRuntime().availableProcessors();
    long maxHeap = Runtime.getRuntime().maxMemory();

    boolean concurrentOverride = false, overridePresent = false;
    String overrideValue = System.getProperty("okio.pool.concurrent");
    if (overrideValue != null) {
      overridePresent = true;
      concurrentOverride = Boolean.valueOf(overrideValue);
    }

    boolean concurrentEnv = overridePresent ?
                            concurrentOverride :
                            (cpuCount >= cpuCountThreshold || maxHeap >= heapSizeThreshold);

    bestPool = concurrentEnv ? concurrentPool() : linkedPool();
  }

  /**
   * Returns a shared segment pool most suitable for use in this runtime.
   */
  static SegmentPool commonPool() {
    return bestPool;
  }

}
