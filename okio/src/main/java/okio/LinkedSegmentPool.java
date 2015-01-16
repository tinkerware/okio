/*
 * Copyright (C) 2014 Square, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package okio;

/**
 * A collection of unused segments, necessary to avoid GC churn and zero-fill.
 * This pool is a thread-safe static singleton.
 */
final class LinkedSegmentPool implements SegmentPool {
  static final LinkedSegmentPool INSTANCE = new LinkedSegmentPool();

  /** Singly-linked list of segments. */
  private Segment next;

  /** Recorder for pool metrics. */
  private final PoolMetrics.Recorder recorder;

  /**
   * Total bytes in this pool. We still maintain this to provide an exact
   * limit on pool memory usage.
   */
  long byteCount;

  private LinkedSegmentPool() {
    recorder = new PoolMetrics.Recorder();
  }

  @Override public Segment take() {
    Segment result = null;
    synchronized (this) {
      if (next != null) {
        result = next;
        next = result.next;
        result.next = null;
        byteCount -= Segment.SIZE;
      }
    }
    if (result != null) {
      recorder.recordSegmentTake(Segment.SIZE, false);
      return result;
    }

    recorder.recordSegmentTake(Segment.SIZE, true);
    return new Segment(); // Pool is empty. Don't zero-fill while holding a lock.
  }

  @Override public void recycle(Segment segment) {
    if (segment.next != null || segment.prev != null) throw new IllegalArgumentException();

    synchronized (this) {
      if (byteCount + Segment.SIZE > MAX_SIZE) {
        recorder.recordSegmentRecycle(Segment.SIZE, true);
        return; // Pool is full.
      }
      byteCount += Segment.SIZE;
      segment.next = next;
      segment.pos = segment.limit = 0;
      next = segment;
    }
    recorder.recordSegmentRecycle(Segment.SIZE, false);
  }

  @Override public PoolMetrics metrics() {
    return recorder.snapshot();
  }
}
