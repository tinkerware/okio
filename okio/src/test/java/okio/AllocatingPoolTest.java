package okio;

import java.util.Arrays;
import java.util.Collection;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import static org.junit.Assert.assertEquals;

/**
 * Common tests for {@link okio.LinkedSegmentPool} {@link okio.ArenaSegmentPool}.
 */
@RunWith(Parameterized.class)
public class AllocatingPoolTest {

  public static class TestedPool {
    final String name;
    final AllocatingPool value;

    TestedPool(String name, AllocatingPool pool) {
      this.name = name;
      this.value = pool;
    }

    @Override public String toString() {
      return name;
    }
  }

  @Parameters(name = "{index}: {0}")
  public static Collection<TestedPool[]> pools() {
    return Arrays.asList(new TestedPool[][] { { new TestedPool("linked", new LinkedSegmentPool()) },
                                              { new TestedPool("arena", new ArenaSegmentPool()) } });
  }

  private AllocatingPool pool;

  public AllocatingPoolTest(TestedPool pool) {
    this.pool = pool.value;
  }

  @Before public void setPool() {
    Util.installPool(pool);
  }

  @After public void restoreCommonPool() {
    Util.installPool((AllocatingPool) SegmentPools.commonPool());
  }

  @Test public void fillAndDrainPool() throws Exception {
    Buffer buffer = new Buffer();

    // Take 2 * MAX_SIZE segments. This will drain the pool, even if other tests filled it.
    buffer.write(new byte[(int) AllocatingPool.MAX_SIZE]);
    buffer.write(new byte[(int) AllocatingPool.MAX_SIZE]);
    assertEquals(0, pool.metrics().usedByteCount());

    // Recycle MAX_SIZE segments. They're all in the pool.
    buffer.readByteString(AllocatingPool.MAX_SIZE);
    assertEquals(AllocatingPool.MAX_SIZE, pool.metrics().usedByteCount());

    // Recycle MAX_SIZE more segments. The pool is full so they get garbage collected.
    buffer.readByteString(AllocatingPool.MAX_SIZE);
    assertEquals(AllocatingPool.MAX_SIZE, pool.metrics().usedByteCount());

    // Take MAX_SIZE segments to drain the pool.
    buffer.write(new byte[(int) AllocatingPool.MAX_SIZE]);
    assertEquals(0, pool.metrics().usedByteCount());

    // Take MAX_SIZE more segments. The pool is drained so these will need to be allocated.
    buffer.write(new byte[(int) AllocatingPool.MAX_SIZE]);
    assertEquals(0, pool.metrics().usedByteCount());
  }

}
