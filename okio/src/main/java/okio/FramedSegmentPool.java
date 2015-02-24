package okio;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.codehaus.mojo.animal_sniffer.IgnoreJRERequirement;

import okio.pool.MetricsRecorder;
import okio.pool.PoolMetrics;
import okio.pool.RecorderSet;

/**
 * A segment pool that allocates segments from thread-local arenas.
 */
class FramedSegmentPool implements AllocatingPool {

  private static final Logger logger = Logger.getLogger(FramedSegmentPool.class.getName());

  static final long MAX_ARENA_SIZE = AllocatingPool.MAX_SIZE;

  static final int MAX_ARENA_COUNT = Runtime.getRuntime().availableProcessors() / 4;

  static final int ARENA_SWEEP_PERIOD_MILLIS = 5 * 100;

  private static final ScheduledExecutorService scheduler;

  static {
    ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1, new ThreadFactory() {

      @Override public Thread newThread(Runnable r) {
        Thread thread = new Thread(r, "Okio Pool Worker");
        thread.setDaemon(true);
        return thread;
      }
    });

    // We make sure the scheduler can be shutdown during finalization.
    executor.setKeepAliveTime(ARENA_SWEEP_PERIOD_MILLIS * 2, TimeUnit.MILLISECONDS);
    executor.allowCoreThreadTimeOut(true);

    scheduler = Executors.unconfigurableScheduledExecutorService(executor);
  }

  /**
   * The basic unit of allocation and reclamation for segments.
   */
  private static abstract class Frame {

    private static final ArenaSegment[] EMPTY_SEGMENTS = new ArenaSegment[0];

    private final static Frame NIL = new NilFrame();

    static Frame nil() {
      return NIL;
    }

    private static class NilFrame extends Frame {

      private NilFrame() {
        super(null, EMPTY_SEGMENTS);
      }

      @Override ArenaSegment take(CacheCounters counters) {
        throw new UnsupportedOperationException("EmptyFrame");
      }

      @Override void recycleNow(ArenaSegment segment, CacheCounters counters) {
        throw new UnsupportedOperationException("EmptyFrame");
      }

      @Override void recycleLater(ArenaSegment segment, CacheCounters metrics) {
        throw new UnsupportedOperationException("EmptyFrame");
      }

      @Override void reclaim() {
        // nothing
      }

      @Override Frame linkFirst(Frame newHead) {
        if (newHead.isNil()) return this;
        newHead.setNextNil();
        return newHead;
      }

      @Override Frame linkLast(Frame newTail) {
        return linkFirst(newTail);
      }

      @Override Frame pop() { return this; }

      @Override Frame next() { return this; }

      @Override Frame setNextNil() { return this; }

      @Override boolean isNil() { return true; }

      @Override boolean isEmpty() { return true; }

      @Override boolean isFresh() { return false; }

      @Override boolean isReusable() { return false; }
    }

    private static class LinkedFrame extends Frame {
      private Frame next = nil();

      LinkedFrame(Arena arena, int capacity) {
        super(arena, new ArenaSegment[capacity]);
      }

      @Override Frame linkFirst(Frame newHead) {
        if (newHead.isNil()) return this;
        ((LinkedFrame) newHead).next = this;
        return newHead;
      }

      @Override Frame linkLast(Frame newTail) {
        if (newTail.isNil()) return this;
        ((LinkedFrame) newTail).next = next;
        next = newTail;
        return newTail;
      }

      @Override Frame pop() {
        Frame result = null;
        if (!next.isNil()) {
          result = next;
          next = nil();
        }
        return result;
      }

      @Override Frame next() {
        return next;
      }

      @Override Frame setNextNil() {
        next = nil();
        return this;
      }
    }

    static Frame create(Arena arena, int capacity) {
      if (arena == null) throw new NullPointerException("arena");
      if (capacity <= 0 || capacity >= Long.SIZE) {
        throw new IllegalArgumentException("capacity: " + capacity);
      }

      return new LinkedFrame(arena, capacity);
    }

    static Frame merge(Arena arena, Frame fragments, int targetCapacity) {
      Frame mergedFrame = new LinkedFrame(arena, targetCapacity);
      int index = 0;
      for (Frame fragment = fragments; !fragment.isNil(); fragment = fragment.next()) {
        System.arraycopy(fragment.segments, 0, mergedFrame.segments, index, fragment.capacity());
        for (ArenaSegment segment : fragment.segments) {
          segment.setFrame(mergedFrame, index++);
        }
      }
      assert index == targetCapacity :
          String.format("index == targetCapacity: %d ≠ %d", index, targetCapacity);

      return mergedFrame;
    }

    private static final long MAP_EMPTY = -1L;
    private static final long MAP_FRESH = 0L;

    static {
      assert Long.bitCount(MAP_EMPTY) == Long.SIZE : "MAP_EMPTY";
      assert Long.bitCount(MAP_FRESH) == 0 : "MAP_FRESH";
    }

    /**
     * A bitmap of segments representing every taken segment with a one bit.
     * Updated by at most one thread at a time; the local thread of the cache
     * when this frame is assigned to one, or the sweeper when it is in the
     * partial list of the arena.
     */
    volatile long taken = MAP_FRESH;

    /**
     * A bitmap of segments representing every recycled segment with a one bit.
     * Can be atomically updated by any thread.
     */
    volatile long recycled = MAP_FRESH;

    private final long mask;

    private final Arena arena;

    private final ArenaSegment[] segments;

    Frame(Arena arena, ArenaSegment[] segments) {
      assert isPowerOfTwo(segments.length) : "segments.length not power-of-two";
      this.mask = segments.length == Long.SIZE ? MAP_EMPTY : (1L << segments.length) - 1;
      this.arena = arena;
      this.segments = segments;
    }

    private long map() {
      long currentRecycled = recycled;
      long currentTaken = taken;
      return currentTaken ^ currentRecycled;
    }

    private String mapToHex(String message, long currentTaken, long currentRecycled) {
      return String.format("%s: %s (+) %s",
                           message,
                           Long.toBinaryString(currentTaken),
                           Long.toBinaryString(currentRecycled));
    }

    private long mergeAndResetMap() {
      long currentRecycled = atomicRecycled.getAndSet(this, 0);
      return taken ^ currentRecycled;
    }

    // Returns a free segment.
    ArenaSegment take(CacheCounters counters) {
      ArenaSegment result = null;
      // Capture the current map of taken segments and avoid accessing it again.
      final long current = taken;

      long merged = current != mask ? current : mergeAndResetMap();
      if (merged != mask) {
        // We have at least one zero bit in the map corresponding to the free
        // segment. We can just turn the rightmost 0-bit on to mark it as
        // in use. See Hacker's Delight, 2-1.
        long used = merged | (merged + 1);
        long delta = used ^ merged;

        assert (merged ^ delta) == merged + delta :
            mapToHex("mismatched take/recycle", merged, delta);

        merged = used;
        result = allocateIfNeeded(delta, counters);
      }

      // Always write back the merged value for taken; otherwise sibling
      // updates to `recycled` are lost.
      taken = merged;
      return result;
    }

    private ArenaSegment allocateIfNeeded(long delta, CacheCounters counters) {
      assert isPowerOfTwo(delta) : "delta not power-of-two";

      // We compute the position of the single bit that was turned on
      // as the log2 of `used XOR merged`; since that number is always
      // a power of two we can round up or down; we choose up here.
      // See Hacker's Delight, 3-2.
      int index = Long.SIZE - Long.numberOfLeadingZeros(delta - 1);
      assert index >= 0 && index < Long.SIZE : "index: " + index;

      ArenaSegment segment = segments[index];
      if (segment != null) {
        return counters.recordReusedSegment(segment);
      }

      return allocateSegmentAtIndex(index, counters);
    }

    private ArenaSegment allocateSegmentAtIndex(int index, CacheCounters counters) {
      ArenaSegment segment = new ArenaSegment(this, index);
      counters.recordAllocation(Segment.SIZE);
      segments[index] = segment;
      return segment;
    }

    // Makes a segment available for later use.
    void recycleNow(ArenaSegment segment, CacheCounters counters) {
      segment.checkParent(this);
      if (!segment.isReusable()) return;
      assert segment == segments[segment.index] : "segments[segment.index]";

      counters.recordLocalRecycledSegment(Segment.SIZE, false);
      long delta = 1L << segment.index;
      assert (taken & delta) == delta : mapToHex("double free", taken, delta);
      taken ^= delta;
    }

    void recycleLater(ArenaSegment segment, CacheCounters counters) {
      segment.checkParent(this);
      if (!segment.isReusable()) return;
      assert segment == segments[segment.index] : "segments[segment.index]";

      counters.recordLocalRecycledSegment(Segment.SIZE, false);
      long delta = 1L << segment.index;
      long recycled = atomicRecycled.getAndAdd(this, delta);
      assert (recycled & delta) == 0 : mapToHex("double free", recycled, delta);
    }

    void reclaim() {
      arena.reclaim(this);
    }

    abstract Frame linkFirst(Frame newHead);

    abstract Frame linkLast(Frame newTail);

    abstract Frame pop();

    abstract Frame next();

    abstract Frame setNextNil();

    boolean isReusable() { return true; }

    boolean isNil() { return false; }

    boolean isEmpty() { return map() == mask; }

    boolean isFresh() { return map() == MAP_FRESH; }

    int usedSegmentCount() { return Long.bitCount(map()); }

    int availableSegmentCount() { return capacity() - usedSegmentCount(); }

    int freshSegmentCount() {
      int available = availableSegmentCount();
      int missing = 0;
      for (ArenaSegment segment : segments) {
        if (segment == null) {
          missing++;
        }
      }
      return available - missing;
    }

    int capacity() { return segments.length; }

    static boolean isPowerOfTwo(long v) {
      return (v & (v - 1)) == 0;
    }

    private final static AtomicLongFieldUpdater<Frame> atomicRecycled =
        AtomicLongFieldUpdater.newUpdater(Frame.class, "recycled");

    /**
     * Tracks only usage of segments and does not attempt to reuse them. Arenas
     * fall back to using tracked frames when they reach their size limits.
     */
    private static class TrackingFrame extends Frame {

      TrackingFrame(Arena arena) {
        super(arena, EMPTY_SEGMENTS);
      }

      @Override Frame linkFirst(Frame newHead) {
        throw new UnsupportedOperationException("linkFirst");
      }

      @Override Frame linkLast(Frame newTail) {
        throw new UnsupportedOperationException("linkLast");
      }

      @Override Frame pop() {
        throw new UnsupportedOperationException("pop");
      }

      @Override Frame setNextNil() {
        throw new UnsupportedOperationException("setNextNil");
      }

      @Override Frame next() { return nil(); }

      @Override boolean isFresh() { return true; }

      @Override boolean isEmpty() { return availableSegmentCount() == 0; }

      @Override boolean isReusable() { return false; }

      @Override int usedSegmentCount() {
        return (int) (taken - recycled);
      }

      @Override int capacity() { return Long.SIZE; }

      @Override ArenaSegment take(CacheCounters counters) {
        taken++;
        counters.recordAllocation(Segment.SIZE);
        return new ArenaSegment(this, -1);
      }

      @Override void recycleNow(ArenaSegment segment, CacheCounters counters) {
        segment.checkParent(this);
        assert !segment.isReusable(): "!segment.isReusable";

        counters.recordLocalRecycledSegment(Segment.SIZE, true);
        long taken = --this.taken;
        assert taken >= recycled : "double free";
      }

      @Override void recycleLater(ArenaSegment segment, CacheCounters counters) {
        segment.checkParent(this);
        assert !segment.isReusable(): "!segment.isReusable";

        counters.recordSiblingRecycledSegment(Segment.SIZE, true);
        long recycled = atomicRecycled.incrementAndGet(this);
        assert taken >= recycled : "double free";
      }

    }
  }

  /**
   * A collection of frames.
   */
  private static class Arena {
    /*
     * We maintain a freelist per each size class (i.e. order), where
     * class `n` is log2 of list size. Initially, we serve frames from
     * the smallest order. Based on measured contention on the list head,
     * we switch to a higher order to balance contention with potential
     * external fragmentation (where a whole frame is held hostage by a
     * straggler segment that is late in being recycling). H/T to Solaris
     * umem that uses the same technique to manage its per-cpu cache
     * structures.
     */

    static final int P2_MIN_ORDER = log2floor(8);
    static final int P2_MAX_ORDER = log2floor(Long.SIZE);
    static final int ORDER_CONTENTION_THRESHOLD = 16;
    static final int ORDER_CONTENTION_INTERVAL = 1024;

    final FrameList[] freeByOrder;
    final FrameList partials;
    final Sweeper sweeper;
    final ArenaCounters metrics;

    volatile int maxOrder = P2_MIN_ORDER;
    volatile long contendedCount = 0;
    volatile long arenaByteCount = 0;
    @SuppressWarnings("UnusedDeclaration")
    volatile long intervalTakeCount = 0;

    Arena() {
      this.freeByOrder = new FrameList[P2_MAX_ORDER];
      for (int order = P2_MIN_ORDER; order < P2_MAX_ORDER; order++) {
        freeByOrder[order] = new FrameList(1 << order, 1 << (order + 1));
      }
      this.partials = new FrameList(1 << P2_MIN_ORDER, 1 << P2_MAX_ORDER);
      this.sweeper = new Sweeper();
      this.metrics = new ArenaCounters();
    }

    // Returns a fresh frame with at least `maxOrder` capacity.
    Frame take() {
      int currentOrder = maxOrder;
      FrameList free = freeByOrder[currentOrder];
      Frame frame = free.pop();
      if (frame.isNil()) {
        frame = allocateFrame(currentOrder);
      }
      assert frame.isFresh() : "frame.isFresh";
      assert frame.capacity() >= (1 << currentOrder) : "frame.capacity > pow2(maxOrder)";
      assert frame.next().isNil() : "frame.next.isNil";

      incrementOrderIfNeeded(currentOrder);
      return frame;
    }

    private Frame allocateFrame(int currentOrder) {
      long arenaBytes = arenaByteCount;
      int capacity = 1 << currentOrder;
      int frameBytes = capacity * Segment.SIZE;
      if (arenaBytes + frameBytes < MAX_ARENA_SIZE) {
        atomicBytes.getAndAdd(this, frameBytes);
        return Frame.create(this, capacity);
      }
      return new Frame.TrackingFrame(this);
    }

    private void incrementOrderIfNeeded(int currentOrder) {
      long intervalTakes = atomicTakes.getAndIncrement(this);

      /*
       * We are guaranteed to have only one thread pass the following
       * condition since fetch-and-add imposes a total order on memory
       * operations.
       *
       * Inside the if block, we have enough time until `intervalTakeCount`
       * wraps around its long size, meaning we can consider this block
       * to be exclusively executed by the current thread, like a lock.
       * The losing threads, unlike a lock, skip the block and do not wait.
       * We discard the updates from those threads to the take and contention
       * counts since we are only interested in keeping track after the
       * new order value is effective.
       */
      if (intervalTakes == ORDER_CONTENTION_INTERVAL) {
        long contended = contendedCount;
        if (currentOrder < P2_MAX_ORDER && contended == ORDER_CONTENTION_THRESHOLD) {
          maxOrder = currentOrder + 1;
          atomicContended.set(this, 0);
        }
        atomicTakes.set(this, 0);
      }
    }

    // Reclaims an empty frame.
    void reclaim(Frame frame) {
      checkFrame(frame);
      if (frame.isReusable()) {
        partials.push(frame);
      }
      // We don't record metrics on discard here because
      // tracking frames do that for us.
    }

    // Visible for testing.
    boolean contains(Frame frame) {
      checkFrame(frame);
      for (Frame current = partials.head; !current.isNil(); current = current.next()) {
        if (frame == current) {
          return true;
        }
      }
      return false;
    }

    long usedBytes() {
      long count = partials.usedByteCount();
      for (int order = P2_MIN_ORDER; order < P2_MAX_ORDER; order++) {
        FrameList free = freeByOrder[order];
        count += free.usedByteCount();
      }
      return count;
    }

    private void checkFrame(Frame frame) {
      if (frame == null) throw new NullPointerException("frame");
      if (frame.isNil()) throw new IllegalArgumentException("frame.isNil");
    }

    private class FrameList {

      private final int minFrameSize;

      private final int maxFrameSize;

      volatile Frame head = Frame.nil();

      FrameList(int minFrameSize, int maxFrameSize) {
        this.minFrameSize = minFrameSize;
        this.maxFrameSize = maxFrameSize;
      }

      boolean casHead(Frame expected, Frame updated) {
        return atomicHead.compareAndSet(this, expected, updated);
      }

      Frame pop() {
        for (;;) {
          Frame current = head;
          if (current.isNil()) return current;

          if (casHead(current, current.next())) {
            return current.setNextNil();
          }
          atomicContended.getAndIncrement(Arena.this);
        }
      }

      void push(Frame frame) {
        int capacity = frame.capacity();
        if (capacity < minFrameSize || capacity >= maxFrameSize) {
          throw new IllegalArgumentException(String.format("%d ≤ frame.capacity (%d) < %d",
                                                           minFrameSize,
                                                           capacity,
                                                           maxFrameSize));
        }
        link(frame, frame);
      }

      void pushAll(Frame head, Frame tail) {
        if (head == null) throw new NullPointerException("head");
        link(head, tail);
      }

      Frame popAll() {
        return link(Frame.nil(), Frame.nil());
      }

      /**
       * Prepends a singly-linked list of frames to this list. {@code head}
       * is the new replacement head for this list; it can not be
       * {@code null}. If it is the nil frame, {@code tail} is ignored
       * and the existing list is cleared.
       * <p>
       * {@code tail} is the tail element of the prepended list to be linked
       * to the head of this list; it can not be {@code null}. If it is the
       * nil frame, the existing frames are discarded and replaced by the
       * given list of frames.
       */
      Frame link(Frame updatedHead, Frame tail) {
        if (updatedHead == null) throw new NullPointerException("head");
        if (tail == null) throw new NullPointerException("tail");

        for (;;) {
          Frame currentHead = this.head;
          currentHead.linkFirst(tail);
          if (casHead(currentHead, updatedHead)) {
            return currentHead;
          }
        }
      }

      long usedByteCount() {
        long count = 0;
        for (Frame current = head; !current.isNil(); current = current.next()) {
          count += Segment.SIZE * current.freshSegmentCount();
        }
        return count;
      }
    }

    class Sweeper {

      synchronized void sweep() {
        /*
         * First, we sweep partial frames and collect the fresh ones (ones
         * that don't have any used segments). Next, we check the current
         * order and collect the frames that are less than the current order.
         * We then coalesce all collected frames so that all frame sizes are
         * at least of `maxOrder`, and push them on to their free lists.
         *
         * Sweeps run concurrently with take and recycle requests. We
         * synchronize on the arena for an additional level of safety against
         * multiple sweep tasks running at the same time instead of blindly
         * trusting that scheduled tasks will not run concurrently.
         *
         * To sweep partial frames, we atomically drain the list of partials,
         * then partition it into fresh and in-use frames and finally prepend
         * the in-use list back to the current (possibly mutated since it was
         * drained) list of partials. The `fresh` and `inUse` lists are built
         * append-only so that they are still in most recently used/reclaimed
         * order, for better GC and cache behavior.
         */

        // First, sweep partials and collect fresh frames for reuse.
        AppendingList[] fresh = new AppendingList[P2_MAX_ORDER];
        for (int i = P2_MIN_ORDER; i < P2_MAX_ORDER; i++) {
          fresh[i] = new AppendingList();
        }

        Frame used = partials.popAll();
        AppendingList inUse = sweepPartials(fresh, used);
        partials.pushAll(inUse.head, inUse.tail);

        // Next, sweep freelists.
        int maxOrder = Arena.this.maxOrder;
        for (int order = P2_MIN_ORDER; order < maxOrder; order++) {
          FrameList free = freeByOrder[order];
          free.pushAll(fresh[order].head, fresh[order].tail);
        }

        // Last step; coalesce frames of smaller than current order.
        // We assume that `maxOrder` never decreases.
        FrameList current = freeByOrder[maxOrder];
        for (int order = P2_MIN_ORDER; order < maxOrder; order++) {
          FrameList free = freeByOrder[order];
          AppendingList coalesced = new AppendingList();

          for (Frame fragment = free.popAll(); !fragment.isNil();) {
            fragment = coalesce(coalesced, fragment, maxOrder, order);
          }

          current.pushAll(coalesced.head, coalesced.tail);
        }
      }

      private AppendingList sweepPartials(AppendingList[] fresh, Frame partials) {
        AppendingList inUse = new AppendingList();
        for (Frame current = partials; !current.isNil(); ) {
          assert current.isReusable() : "current.isReusable";
          AppendingList collected = inUse;
          if (current.isFresh()) {
            collected = fresh[log2floor(current.capacity())];
          }
          current = collected.append(current);
        }
        return inUse;
      }

      private Frame coalesce(AppendingList coalesced,
                             Frame fragment,
                             int coalesceOrder,
                             int fragmentOrder) {

        AppendingList collected = new AppendingList();
        Frame remaining = fragment;
        for (int remainingCount = coalesceOrder / fragmentOrder;
             remainingCount > 0; remainingCount--) {

          if (remaining.isNil()) {
            // Discard any remainder frames; too small to coalesce.
            metrics.recordDiscard(collected.segmentCount());
            collected.clear();
            break;
          }

          Frame frame = remaining;
          remaining = remaining.pop();

          assert frame.isFresh() : "frame.isFresh";
          collected.append(frame);
        }

        if (!collected.isEmpty()) {
          coalesced.append(Frame.merge(Arena.this, collected.head, 1 << coalesceOrder));
        }
        return remaining;
      }
    }

    private static class AppendingList {
      Frame head = Frame.nil();
      Frame tail = Frame.nil();
      int segmentCount = 0;

      Frame append(Frame frame) {
        Frame next = frame.next();
        tail = tail.linkLast(frame);
        if (head.isNil()) {
          head = tail;
        }
        segmentCount += frame.capacity();
        return next;
      }

      void clear() {
        head = tail = Frame.nil();
        segmentCount = 0;
      }

      boolean isEmpty() {
        return segmentCount == 0;
      }

      int segmentCount() {
        return segmentCount;
      }
    }

    // log2, rounding down.
    static int log2floor(int i) {
      // Hacker's Delight, 3-2
      if (i <= 0) throw new IllegalArgumentException("i > 0");
      return Integer.SIZE - Integer.numberOfLeadingZeros(i) - 1;
    }

    private final static AtomicReferenceFieldUpdater<FrameList, Frame> atomicHead =
        AtomicReferenceFieldUpdater.newUpdater(FrameList.class, Frame.class, "head");
    private final static AtomicLongFieldUpdater<Arena> atomicContended =
        AtomicLongFieldUpdater.newUpdater(Arena.class, "contendedCount");
    private final static AtomicLongFieldUpdater<Arena> atomicTakes =
        AtomicLongFieldUpdater.newUpdater(Arena.class, "intervalTakeCount");
    private final static AtomicLongFieldUpdater<Arena> atomicBytes =
        AtomicLongFieldUpdater.newUpdater(Arena.class, "arenaByteCount");
  }

  /**
   * Frames track their segments; a segment is always recycled at its owner
   * frame. This avoids a pitfall of thread-local allocators where less busy
   * threads (or worse, low priority background tasks) may consume a buffer
   * created by a busy thread and end up recycling segments on their own
   * arenas, away from where they are needed.
   */
  private static class ArenaSegment extends Segment {

    private Frame parent;
    private int index;

    ArenaSegment(Frame parent, int index) {
      setFrame(parent, index);
    }

    void setFrame(Frame parent, int index) {
      this.parent = parent;
      this.index = index;
    }

    boolean isReusable() {
      return index >= 0;
    }

    public void checkParent(Frame frame) {
      if (this.parent != frame) throw new IllegalArgumentException("frame");
    }
  }

  /**
   * A thread local cache that handles take and recycle requests for segments.
   */
  @IgnoreJRERequirement
  class SegmentCache {

    private final Arena arena;

    private final long threadId;

    private final CacheCounters metrics;

    private final CacheRef selfRef;

    private Frame top;

    private Frame bottom;

    SegmentCache(Arena arena, long threadId) {
      this.threadId = threadId;
      this.arena = arena;
      this.metrics = new CacheCounters(arena.metrics);
      this.top = Frame.nil();
      this.bottom = Frame.nil();
      this.selfRef = new CacheRef(this);
    }

    CacheRef ref() {
      return selfRef;
    }

    /**
     * Reserves a free segment or allocates a new segment. Only called from
     * the local thread. This operation has O(1) time complexity and does not
     * allocate any objects other than the segment (if necessary).
     */
    Segment take() {
      Frame initialTop = top;
      Frame initialBottom = bottom;

      if (initialTop.isEmpty()) {
        ensureTopAvailable(initialBottom);
      }
      assert !top.isEmpty(): "!top.isEmpty";
      assert frameExclusive(top) && frameExclusive(bottom) : "framesExclusive";
      assert framesPreserved(initialTop, initialBottom) : "framesPreserved";

      ArenaSegment result = top.take(metrics);
      assert result != null : "segment unavailable";
      return result;
    }

    /**
     * Makes sure that the top frame is not empty. This is part of the slow
     * path on a segment take; we make it into a separate method so that the
     * fast path has a bigger chance of getting inlined.
     */
    private void ensureTopAvailable(Frame candidate) {
      Frame spare = candidate;
      if (spare.isEmpty()) {
        spare = arena.take();
      }
      takeBottomAndRotateTop(spare).reclaim();
    }

    /**
     * Makes the given frame top and rotates the existing top frame to bottom,
     * returning the initial bottom, or the empty frame if the given frame was
     * the initial bottom.
     */
    private Frame takeBottomAndRotateTop(Frame availableOrBottom) {
      Frame oldTop = top;
      Frame oldBottom = bottom;
      top = availableOrBottom;
      bottom = oldTop;
      return oldBottom == availableOrBottom ? Frame.nil() : oldBottom;
    }

    private boolean frameExclusive(Frame frame) {
      return frame.isNil() || !arena.contains(frame);
    }

    private boolean framesPreserved(Frame initialTop, Frame initialBottom) {
      return initialTop == top ||
             (initialTop == bottom && (initialBottom.isNil() || arena.contains(initialBottom)));
    }

    /**
     * Recycles a segment. Only called from the local thread.
     */
    void recycle(ArenaSegment segment) {
      if (segment == null) throw new NullPointerException("segment");

      segment.reset();
      Frame frame = segment.parent;
      if (frame == bottom || frame == top) {
        frame.recycleNow(segment, metrics);
      } else {
        frame.recycleLater(segment, metrics);
      }
    }

    @Override public String toString() {
      return String.format("SegmentCache[tid=%s, identity=%s, size=%,3d]",
                           Long.toHexString(threadId),
                           super.toString(),
                           metrics.usedBytes());
    }
  }

  /**
   * A weak reference to a segment cache so that the pool can avoid holding on
   * to caches whose threads are dead but still access their metrics.
   */
  private static class CacheRef extends WeakReference<SegmentCache> {

    private final CacheCounters metrics;

    public CacheRef(SegmentCache enclosing) {
      super(enclosing);
      this.metrics = enclosing.metrics;
    }

    CacheCounters counters() {
      return metrics;
    }

    boolean isAlive() {
      return get() != null;
    }
  }

  // Visible for testing.
  final ThreadLocal<SegmentCache> cache = new ThreadLocal<SegmentCache>() {
    @Override protected SegmentCache initialValue() {
      SegmentCache cache = createCache();
      allCaches.add(cache.ref());
      return cache;
    }
  };

  private final Collection<CacheRef> allCaches = new CopyOnWriteArrayList<>();

  private final CopyOnWriteArrayList<Arena> arenas;

  private final GlobalMetrics globalCounters;

  /**
   * Periodic task sweeping caches and arenas for maintenance. Can be
   * {@code null} in unit tests to disable the task.
   */
  final ScheduledFuture<?> taskHandle;

  private volatile boolean running = true;

  // Visible for testing.
  FramedSegmentPool() {
    this.globalCounters = new GlobalMetrics();
    Arena[] arenas = new Arena[MAX_ARENA_COUNT];
    for (int i = 0; i < arenas.length; i++) {
      arenas[i] = new Arena();
    }
    this.arenas = new CopyOnWriteArrayList<>(arenas);
    this.taskHandle = createSweepTask().schedule();
  }

  @Override public Segment take() {
    checkState();
    return cache.get().take();
  }

  @Override public void recycle(Segment segment) {
    if (segment == null) throw new NullPointerException("segment");

    /*
     * We tolerate foreign segments to avoid hard-to-debug issues if
     * buffers created prior to a Util.installPool call were used
     * alongside buffers created later.
     */
    if (!(running && segment instanceof ArenaSegment)) return;

    ArenaSegment arenaSegment = (ArenaSegment) segment;
    cache.get().recycle(arenaSegment);
  }

  @Override public PoolMetrics metrics() {
    return globalCounters.aggregate();
  }

  @Override public void shutdown() {
    running = false;

    if (taskHandle != null && !taskHandle.isCancelled()) {
      if (taskHandle.isDone()) {
        // Cleaner task had failed and we are now noticing.
        Throwable thrown = null;
        try {
          taskHandle.get();
        } catch (InterruptedException e) {
          Error error = new AssertionError("Pool worker is done");
          error.initCause(e);
          throw error;
        } catch (ExecutionException e) {
          thrown = e;
        }
        logger.log(Level.WARNING,
                   "Found previous pool worker failure; continuing pool shut down",
                   thrown);
      } else {
        taskHandle.cancel(false);
      }

      if (!allCaches.isEmpty()) {
        logger.warning("Threads that used Okio must die before this pool is reclaimed by GC.");
      }
    }
  }

  private void checkState() {
    if (!running) throw new IllegalStateException("!running");
    if (elapsedTimeSinceLastSweepTooLong()) {
      try {
        // Don't let a failure of the sweep task go unnoticed; fail fast.
        taskHandle.get(ARENA_SWEEP_PERIOD_MILLIS, TimeUnit.MILLISECONDS);
      } catch (InterruptedException | ExecutionException | TimeoutException e) {
        shutdown();
      }
    }
  }

  // Visible for testing.
  boolean elapsedTimeSinceLastSweepTooLong() {
    return taskHandle.getDelay(TimeUnit.MILLISECONDS) > 2 * ARENA_SWEEP_PERIOD_MILLIS;
  }

  // Visible for testing.
  int cacheCount() {
    return allCaches.size();
  }

  // Visible for testing.
  SweepTask createSweepTask() {
    return new SweepTask();
  }

  private SegmentCache createCache() {
    long threadId = Thread.currentThread().getId();
    int arenaIndex = (int) Math.abs(threadId % arenas.size());
    Arena arena = arenas.get(arenaIndex);
    return new SegmentCache(arena, threadId);
  }


  class SweepTask implements Runnable {

    @Override public void run() {
      expungeStaleCacheRefs();
      reclaimAndCompactArenaFrames();
    }

    public ScheduledFuture<?> schedule() {
      return scheduler.scheduleWithFixedDelay(this,
                                              ARENA_SWEEP_PERIOD_MILLIS,
                                              ARENA_SWEEP_PERIOD_MILLIS,
                                              TimeUnit.MILLISECONDS);
    }

  }

  interface Callback<V, U> {

    U onValue(V value);
  }

  private <V, U, C extends Collection<U>> void sweep(Callback<V, U> transformer,
                                                     Iterable<? extends V> values,
                                                     C collected) {
    for (V value : values) {
      U transformed = transformer.onValue(value);
      if (transformed != null) {
        collected.add(transformed);
      }
    }
  }

  private void expungeStaleCacheRefs() {
    Collection<CacheRef> toRemove = new ArrayList<>();
    sweep(new Callback<CacheRef, CacheRef>() {
      @Override public CacheRef onValue(CacheRef cacheRef) {
        if (!cacheRef.isAlive()) {
          globalCounters.recordReclaimedCache(cacheRef.counters());
          return cacheRef;
        }
        return null;
      }
    }, allCaches, toRemove);

    if (!toRemove.isEmpty()) {
      // We assume that there are many more active caches than
      // inactive ones. The following has O(N^2) time and
      // O(N) space complexity in COW list, so it may behave
      // unpredictably where thread creation and death rates are
      // very high.
      allCaches.removeAll(toRemove);
    }
  }

  private void reclaimAndCompactArenaFrames() {
    sweep(new Callback<Arena, Void>() {
      @Override public Void onValue(Arena arena) {
        arena.sweeper.sweep();
        return null;
      }
    }, arenas, null);
  }

  /**
   * Returns the latest total count of bytes used by all caches and
   * arenas at this point in time. This count is useful for informational
   * and debugging purposes only; the returned value is <em>NOT</em> an
   * atomic snapshot. Invocation in the absence of concurrent updates
   * returns an accurate result, but concurrent updates that occur while
   * the sum is being calculated might not be incorporated.
   */
  private long latestUsedBytes() {
    final AtomicLong total = new AtomicLong();
    sweep(new Callback<CacheRef, Void>() {
      @Override public Void onValue(CacheRef cache) {
        total.getAndAdd(cache.counters().usedBytes());
        return null;
      }
    }, allCaches, null);

    sweep(new Callback<Arena, Void>() {
      @Override public Void onValue(Arena arena) {
        total.getAndAdd(arena.usedBytes());
        return null;
      }
    }, arenas, null);
    return total.get();
  }

  PoolMetrics latestCounters() {
    final long usedBytes = latestUsedBytes();
    final PoolMetrics result = globalCounters.aggregate();
    return new PoolMetrics() {
      @Override public long usedByteCount() {
        return usedBytes;
      }

      @Override public long allocatedByteCount() {
        return result.allocatedByteCount();
      }

      @Override public long outstandingByteCount() {
        return result.outstandingByteCount();
      }

      @Override public long totalAllocationCount() {
        return result.totalAllocationCount();
      }

      @Override public long totalTakeCount() {
        return result.totalTakeCount();
      }

      @Override public long totalRecycleCount() {
        return result.totalRecycleCount();
      }
    };
  }

  private static class ArenaCounters {

    /** Source of all counter recorders used by this arena and associated caches. */
    private final RecorderSet recorders = new RecorderSet();
    /** Recorder to capture activity that happens outside the caches. */
    private final MetricsRecorder recorder = recorders.multiWriterRecorder();

    public void recordDiscard(int segmentCount) {
      recorder.recordTrim(Segment.SIZE * segmentCount);
    }

    public PoolMetrics aggregate() {
      return recorders.aggregate();
    }
  }

  private static class CacheCounters {

    private final MetricsRecorder local;
    private final MetricsRecorder siblings;
    private final ArenaCounters parent;

    /**
     * The total number of free bytes currently in the cache; never negative.
     */
    @SuppressWarnings("UnusedDeclaration")
    private volatile long byteCount;

    private CacheCounters(ArenaCounters parent) {
      this.parent = parent;
      this.local = parent.recorders.singleWriterRecorder();
      this.siblings = parent.recorders.multiWriterRecorder();
    }

    long usedBytes() {
      return byteCount;
    }

    ArenaSegment recordReusedSegment(ArenaSegment segment) {
      if (segment != null) {
        local.recordUse(Segment.SIZE, false);
        long updatedCacheSize = atomicBytes.addAndGet(this, -Segment.SIZE);
        assert updatedCacheSize >= 0 : String.format("updatedCacheSize >= 0 : %d, %s",
                                                     updatedCacheSize,
                                                     toString());
      }
      return segment;
    }

    void recordAllocation(int segmentSize) {
      local.recordUse(segmentSize, true);
    }

    void recordLocalRecycledSegment(int segmentSize, boolean deallocated) {
      if (!deallocated) {
        atomicBytes.getAndAdd(this, segmentSize);
      }

      local.recordRecycle(segmentSize, deallocated);
    }

    void recordSiblingRecycledSegment(int segmentSize, boolean deallocated) {
      if (!deallocated) {
        atomicBytes.getAndAdd(this, segmentSize);
      }

      siblings.recordRecycle(segmentSize, deallocated);
    }

    PoolMetrics detachAndSnapshot() {
      return parent.recorders.remove(Arrays.asList(local, siblings));
    }

    private static final AtomicLongFieldUpdater<CacheCounters> atomicBytes =
        AtomicLongFieldUpdater.newUpdater(CacheCounters.class, "byteCount");
  }

  class GlobalMetrics {

    // Guarded by this GlobalMetrics instance.
    private PoolMetrics baselineMetrics = PoolMetrics.zero();

    // Guarded by this GlobalMetrics instance.
    private final List<CacheCounters> reclaimedCaches = new LinkedList<>();

    public PoolMetrics aggregate() {
      final Collection<PoolMetrics> arenaCounters = new ArrayList<>();
      final Collection<PoolMetrics> cacheCounters = getAndResetReclaimedCaches();

      sweep(new Callback<Arena, PoolMetrics>() {
        @Override public PoolMetrics onValue(Arena arena) {
          return arena.metrics.aggregate();
        }
      }, arenas, arenaCounters);

      PoolMetrics arenaAggregate = PoolMetrics.merge(arenaCounters);
      PoolMetrics reclaimedAggregate = PoolMetrics.merge(cacheCounters);

      /*
       * We are assured that the new reclaimed counters is a delta (disjoint
       * with any other deltas in concurrent aggregations) due to the atomic
       * sweep of arenas and atomic reset of reclaimed cache counters;
       * otherwise merge would count duplicates.
       */
      return arenaAggregate.merge(updateBaselineMetrics(reclaimedAggregate));
    }

    synchronized private PoolMetrics updateBaselineMetrics(PoolMetrics delta) {
      baselineMetrics = baselineMetrics.merge(delta);
      return baselineMetrics;
    }

    synchronized void recordReclaimedCache(CacheCounters metrics) {
      reclaimedCaches.add(metrics);
    }

    private Collection<PoolMetrics> getAndResetReclaimedCaches() {
      List<CacheCounters> caches;
      synchronized (this) {
        caches = new ArrayList<>(reclaimedCaches);
        reclaimedCaches.clear();
      }

      Collection<PoolMetrics> metrics = new ArrayList<>();
      for (CacheCounters cache : caches) {
        metrics.add(cache.detachAndSnapshot());
      }
      return metrics;
    }
  }
}
