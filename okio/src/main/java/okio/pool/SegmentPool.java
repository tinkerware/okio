package okio.pool;

/**
 * A collection of unused segments, necessary to avoid GC churn and zero-fill.
 * Pool implementations are thread-safe.
 */
public interface SegmentPool {

  /**
   * A snapshot of metrics summarizing usage of the pool up to now.
   */
  PoolMetrics metrics();
}
