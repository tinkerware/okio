package okio;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A segment pool that performs well when buffer take and recycle operations are concurrent.
 */
class ConcurrentSegmentPool implements SegmentPool {
  static final ConcurrentSegmentPool INSTANCE = new ConcurrentSegmentPool();

  private final Queue<Segment> free = new ConcurrentLinkedQueue<>();

  private final AtomicLong byteCount = new AtomicLong();

  @Override public Segment take() {
    Segment segment = free.poll();
    if (segment == null) {
      segment = new Segment();
    } else {
      long bytesInPool = byteCount.addAndGet(-Segment.SIZE);
      assert bytesInPool >= 0 : "Bytes in pool must be non-negative";
    }
    return segment;
  }

  @Override public void recycle(Segment segment) {
    if (segment.next != null || segment.prev != null) throw new IllegalArgumentException();
    if (byteCount.get() + Segment.SIZE > MAX_SIZE) return;

    byteCount.addAndGet(Segment.SIZE);
    segment.next = null;
    segment.pos = segment.limit = 0;

    free.offer(segment);
  }
}
