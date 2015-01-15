package okio;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * A segment pool that performs well when buffer take and recycle operations are concurrent.
 */
class ConcurrentSegmentPool implements SegmentPool {
  static final ConcurrentSegmentPool INSTANCE = new ConcurrentSegmentPool();

  private final Queue<Segment> free = new ConcurrentLinkedQueue<>();

  private final PoolMetrics.Recorder recorder = new PoolMetrics.Recorder();

  @Override public Segment take() {
    Segment segment = free.poll();
    if (segment == null) {
      segment = new Segment();
      recorder.recordSegmentTake(Segment.SIZE, true);
    } else {
      recorder.recordSegmentTake(Segment.SIZE, false);
    }
    return segment;
  }

  @Override public void recycle(Segment segment) {
    if (segment.next != null || segment.prev != null) throw new IllegalArgumentException();
    if (recorder.usedByteCount.sum() + Segment.SIZE > MAX_SIZE) {
      recorder.recordSegmentRecycle(Segment.SIZE, true);
      return;
    }

    recorder.recordSegmentRecycle(Segment.SIZE, false);
    segment.next = null;
    segment.pos = segment.limit = 0;

    free.offer(segment);
  }

  @Override public PoolMetrics metrics() {
    return recorder.snapshot();
  }
}
