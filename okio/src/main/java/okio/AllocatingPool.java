package okio;

import okio.pool.SegmentPool;

/**
 * A segment pool that can actually allocate and manage segments.
 */
interface AllocatingPool extends SegmentPool {

  /** The maximum number of bytes to pool. */
  // TODO: Is 64 KiB a good maximum size? Do we ever have that many idle segments?
  long MAX_SIZE = 64 * 1024; // 64 KiB.

  Segment take();

  void recycle(Segment segment);

}
