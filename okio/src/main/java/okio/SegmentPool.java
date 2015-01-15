package okio;

/**
 * A collection of unused segments, necessary to avoid GC churn and zero-fill.
 * Pool implementations must be thread-safe.
 */
public interface SegmentPool {

  /** The maximum number of bytes to pool. */
  // TODO: Is 64 KiB a good maximum size? Do we ever have that many idle segments?
  long MAX_SIZE = 64 * 1024; // 64 KiB.

  Segment take();

  void recycle(Segment segment);

  PoolMetrics metrics();
}
