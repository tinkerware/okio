package com.squareup.okio.benchmarks.profile;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.IterationParams;
import org.openjdk.jmh.profile.InternalProfiler;
import org.openjdk.jmh.profile.ProfilerResult;
import org.openjdk.jmh.results.AggregationPolicy;
import org.openjdk.jmh.results.Result;

import okio.SegmentPools;
import okio.pool.PoolMetrics;
import okio.pool.SegmentPool;

/**
 * Captures Okio pool statistics during benchmark iterations.
 */
public class OkioPoolProfiler implements InternalProfiler {

  @Override
  public void beforeIteration(BenchmarkParams benchmarkParams, IterationParams iterationParams) {
    okio().installPool();
  }

  @Override
  public Collection<? extends Result> afterIteration(BenchmarkParams benchmarkParams,
                                                     IterationParams iterationParams) {

    PoolMetrics metrics = okio().shutdownPool();

    return Arrays.asList(BytesResult.of("@okio.pool.used",
                                        metrics.usedByteCount(),
                                        'K',
                                        AggregationPolicy.SUM),
                         BytesResult.of("@okio.pool.allocated",
                                        metrics.allocatedByteCount(),
                                        'K',
                                        AggregationPolicy.SUM),
                         BytesResult.of("@okio.pool.outstanding",
                                        metrics.outstandingByteCount(),
                                        'K',
                                        AggregationPolicy.SUM),
                         new ProfilerResult("@okio.pool.allocations",
                                            metrics.totalAllocationCount(),
                                            "counts",
                                            AggregationPolicy.MAX),
                         new ProfilerResult("@okio.pool.takes",
                                            metrics.totalTakeCount(),
                                            "counts",
                                            AggregationPolicy.MAX),
                         new ProfilerResult("@okio.pool.recycles",
                                            metrics.totalRecycleCount(),
                                            "counts",
                                            AggregationPolicy.MAX));
  }

  @Override public boolean checkSupport(List<String> msgs) {
    try {
      OkioAccess.create();
      return true;
    }
    catch (Exception e) {
      msgs.add(e.getMessage());
    }
    return false;
  }

  @Override public String label() {
    return "okio_pool";
  }

  @Override public String getDescription() {
    return "Okio Pool Profiler";
  }

  private volatile OkioAccess okio;

  private OkioAccess okio() {
    if (okio == null) {
      try {
        okio = OkioAccess.create();
      }
      catch (Exception e) {
        throw propagate(e);
      }
    }

    return okio;
  }

  static class BytesResult extends ProfilerResult {

    private static final String byteUnits = "BKMGTPE";

    static BytesResult of(String label, long bytes, char byteUnit, AggregationPolicy policy) {
      int unitValue = 1024;
      int exp = Math.max(0, byteUnits.indexOf(byteUnit));
      String unit = exp > 0 ? byteUnit + "iB" : "B";
      double value = bytes / Math.pow(unitValue, exp);
      return new BytesResult(label, value, unit, policy);
    }

    private BytesResult(String label,
                        double n,
                        String unit,
                        AggregationPolicy policy) {
      super(label, n, unit, policy);
    }
  }

  static class OkioAccess {

    private final MethodHandle installPool;
    private final MethodHandle currentPool;
    private final MethodHandle newArenaPool;
    private final MethodHandle shutdownPool;

    static OkioAccess create()
        throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException {

      Class<?> utilClass = Class.forName("okio.Util");
      Class<?> allocatingPoolClass = Class.forName("okio.AllocatingPool");
      Class<?> poolClass = SegmentPools.commonPool().getClass();

      Method installPoolMethod = utilClass.getDeclaredMethod("installPool", allocatingPoolClass);
      installPoolMethod.setAccessible(true);
      Method currentPoolMethod = utilClass.getDeclaredMethod("currentPool");
      currentPoolMethod.setAccessible(true);

      Constructor<?> poolConstructor = poolClass.getDeclaredConstructor();
      poolConstructor.setAccessible(true);
      Method shutdownMethod = poolClass.getDeclaredMethod("shutdown");
      shutdownMethod.setAccessible(true);

      MethodHandles.Lookup lookup = MethodHandles.lookup();
      MethodHandle installPoolHandle = lookup.unreflect(installPoolMethod);
      MethodHandle currentPoolHandle = lookup.unreflect(currentPoolMethod);
      MethodHandle newArenaPoolHandle = lookup.unreflectConstructor(poolConstructor);
      MethodHandle shutdownHandle = lookup.unreflect(shutdownMethod);

      return new OkioAccess(installPoolHandle,
                            currentPoolHandle,
                            newArenaPoolHandle,
                            shutdownHandle);
    }

    OkioAccess(MethodHandle installPool,
               MethodHandle currentPool,
               MethodHandle newArenaPool,
               MethodHandle shutdownPool) {

      this.installPool = installPool;
      this.currentPool = currentPool;
      this.newArenaPool = newArenaPool;
      this.shutdownPool = shutdownPool;
    }

    SegmentPool installPool() {
      try {
        Object pool = newArenaPool.invoke();
        installPool.invoke(pool);
        return (SegmentPool) pool;
      }
      catch (Throwable throwable) {
        throw propagate(throwable);
      }
    }

    PoolMetrics shutdownPool() {
      try {
        SegmentPool pool = (SegmentPool) currentPool.invoke();
        PoolMetrics metrics = pool.metrics();
        shutdownPool.invoke(pool);
        return metrics;
      }
      catch (Throwable t) {
        throw propagate(t);
      }
    }

  }

  private static RuntimeException propagate(Throwable t) {
    if (t instanceof RuntimeException) {
      throw ((RuntimeException) t);
    }
    if (t instanceof Error) {
      throw (Error) t;
    }
    throw new RuntimeException(t);
  }
}
