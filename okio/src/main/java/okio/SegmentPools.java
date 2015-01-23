package okio;

import java.lang.reflect.Constructor;
import java.util.logging.Level;
import java.util.logging.Logger;

import okio.pool.SegmentPool;

/**
 * Provides access to the common segment pool.
 */
public class SegmentPools {
  private static final Logger logger = Logger.getLogger(SegmentPools.class.getName());

  private SegmentPools() { /* not allowed */ }

  /*
   * We act as a factory for segment pools, but the existing two implementations
    * are in a different package. Until that's not the case, we bend the rules
    * a bit and reach into the package local scope to instantiate the pools;
    * this prevents us from exposing implementation details as API.
   */

  private static SegmentPool linkedPool() {
    try {
      return (SegmentPool) create(Class.forName("okio.LinkedSegmentPool"));
    }
    catch (Exception e) {
      AssertionError error =
          new AssertionError("LinkedSegmentPool can not be instantiated");
      error.initCause(e);
      throw error;
    }
  }

  private static SegmentPool concurrentPool() throws Exception {
    return (SegmentPool) create(Class.forName("okio.ArenaSegmentPool"));
  }

  private static <T> T create(Class<T> poolType) throws Exception {
    Constructor<T> constructor = poolType.getDeclaredConstructor();
    constructor.setAccessible(true);
    return constructor.newInstance();
  }

  private static final SegmentPool bestPool;

  static {
    long heapSizeThreshold = 1024 * 1024 * 1024; // 1 GiB
    long cpuCountThreshold = 2;

    int cpuCount = Runtime.getRuntime().availableProcessors();
    long maxHeap = Runtime.getRuntime().maxMemory();

    boolean concurrentOverride = false, overridePresent = false;
    String overrideValue = System.getProperty("okio.pool.concurrent");
    if (overrideValue != null) {
      overridePresent = true;
      concurrentOverride = Boolean.valueOf(overrideValue);
    }

    boolean concurrentEnv = overridePresent ?
                            concurrentOverride :
                            (cpuCount >= cpuCountThreshold && maxHeap >= heapSizeThreshold);

    SegmentPool pool;
    try {
      pool = concurrentEnv ? concurrentPool() : linkedPool();
    }
    catch (Exception e) {
      logger.log(Level.CONFIG, "Concurrent pool not available, using linked pool", e);
      pool = linkedPool();
    }
    bestPool = pool;
  }

  /**
   * Returns a shared segment pool most suitable for use in this runtime.
   */
  public static SegmentPool commonPool() {
    return bestPool;
  }

}
