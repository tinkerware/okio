package okio;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

import static okio.Util.currentPool;

/**
 * Fill/drain tests for linked segment pool.
 */
public class LinkedSegmentPoolTest {

  @Before public void setPool() {
    Util.installPool(new LinkedSegmentPool());
  }

  @After public void restoreCommonPool() {
    Util.installPool((AllocatingPool) SegmentPools.commonPool());
  }

  @Test public void fillAndDrainPool() throws Exception {
    Buffer buffer = new Buffer();

    // Take 2 * MAX_SIZE segments. This will drain the pool, even if other tests filled it.
    buffer.write(new byte[(int) AllocatingPool.MAX_SIZE]);
    buffer.write(new byte[(int) AllocatingPool.MAX_SIZE]);
    assertEquals(0, currentPool().metrics().usedByteCount());

    // Recycle MAX_SIZE segments. They're all in the pool.
    buffer.readByteString(AllocatingPool.MAX_SIZE);
    assertEquals(AllocatingPool.MAX_SIZE, currentPool().metrics().usedByteCount());

    // Recycle MAX_SIZE more segments. The pool is full so they get garbage collected.
    buffer.readByteString(AllocatingPool.MAX_SIZE);
    assertEquals(AllocatingPool.MAX_SIZE, currentPool().metrics().usedByteCount());

    // Take MAX_SIZE segments to drain the pool.
    buffer.write(new byte[(int) AllocatingPool.MAX_SIZE]);
    assertEquals(0, currentPool().metrics().usedByteCount());

    // Take MAX_SIZE more segments. The pool is drained so these will need to be allocated.
    buffer.write(new byte[(int) AllocatingPool.MAX_SIZE]);
    assertEquals(0, currentPool().metrics().usedByteCount());
  }

}
