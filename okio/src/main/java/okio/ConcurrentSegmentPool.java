package okio;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

import okio.pool.PoolMetrics;

/**
 * A segment pool that performs well when buffer take and recycle operations are concurrent.
 */
class ConcurrentSegmentPool implements AllocatingPool {
  static final ConcurrentSegmentPool INSTANCE = new ConcurrentSegmentPool();

  private final Queue<Segment> free = new ConcurrentLinkedQueue<>();

  private final PoolMetrics.Recorder recorder = new PoolMetrics.Recorder();

  private final AtomicLong byteCount = new AtomicLong();

  @Override public Segment take() {
    Segment segment = free.poll();
    if (segment == null) {
      segment = new Segment();
      recorder.recordUse(Segment.SIZE, true);
    } else {
      byteCount.addAndGet(-Segment.SIZE);
      recorder.recordUse(Segment.SIZE, false);
    }
    return segment;
  }

  @Override public void recycle(Segment segment) {
    if (segment.next != null || segment.prev != null) throw new IllegalArgumentException();
    if (byteCount.get() + Segment.SIZE > MAX_SIZE) {
      recorder.recordRecycle(Segment.SIZE, true);
      return;
    }

    segment.next = null;
    segment.pos = segment.limit = 0;

    byteCount.addAndGet(Segment.SIZE);
    recorder.recordRecycle(Segment.SIZE, false);
    free.offer(segment);
  }

  @Override public PoolMetrics metrics() {
    return recorder.snapshot();
  }
}
