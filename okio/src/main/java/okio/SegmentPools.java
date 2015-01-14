package okio;

/**
 * Provides access to common segment pools.
 */
final class SegmentPools {

  private SegmentPools() { /* not allowed */ }

  /**
   * Returns a shared buffer pool most suitable for use in this runtime.
   */
  static LinkedSegmentPool commonPool() {
    return LinkedSegmentPool.INSTANCE;
  }
}
