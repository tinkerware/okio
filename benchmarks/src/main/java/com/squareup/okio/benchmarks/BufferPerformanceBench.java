/*
 * Copyright (C) 2014 Square, Inc. and others.
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

package com.squareup.okio.benchmarks;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import okio.Buffer;
import okio.BufferedSource;
import okio.Okio;
import okio.Sink;
import okio.Timeout;

import static java.util.Objects.requireNonNull;

@Fork(1)
@Warmup(iterations = 10, time = 10)
@Measurement(iterations = 10, time = 30)
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class BufferPerformanceBench {

  public static final File OriginPath =
      new File(System.getProperty("okio.bench.origin.path", "/dev/urandom"));

  public static final int MaxRequests = 1000;

  @Param({ "1000" })
  int maxThinkMicros = 1000;

  @Param({ "987" })
  int maxReadBytes = 987;

  @Param({ "1298" })
  int maxWriteBytes = 1298;

  @Param({ "2109" })
  int requestSize = 2109;

  @Param({ "16" })
  int responseFactor = 16;

  byte[] requestBytes;

  byte[] responseBytes;

  @Setup(Level.Trial)
  public void storeRequestResponseData() throws IOException {
    checkOrigin(OriginPath);

    requestBytes = storeSourceData(new byte[requestSize]);
    responseBytes = storeSourceData(new byte[requestSize * responseFactor]);
  }

  /* Test Workload
   *
   * Each benchmark thread maintains three buffers; a receive buffer, a process buffer
   * and a send buffer. At every operation:
   *
   *   - We fill up the receive buffer using the origin, write the request to the process
   *     buffer, and consume the process buffer.
   *   - We fill up the process buffer using the origin, write the response to the send
   *     buffer, and consume the send buffer.
   *
   * We use an "origin" source that serves as a preexisting sequence of bytes we can read
   * from the file system. The request and response bytes are initialized in the beginning
   * and reused throughout the benchmark in order to eliminate GC effects.
   *
   * Typically, we simulate the usage of small reads and large writes. Requests and
   * responses are satisfied with precomputed buffers to eliminate GC effects on
   * results.
   *
   * There are two types of benchmark tests; hot tests are "pedal to the metal" and
   * use all CPU they can take. These are useful to magnify performance effects of
   * changes but are not realistic use cases that should drive optimization efforts.
   * Cold tests introduce think time between the receiving of the request and sending
   * of the response. They are more useful as a reasonably realistic workload where
   * buffers can be read from and written to during request/response handling but
   * may hide subtle effects of most changes on performance. Prefer to look at the cold
   * benchmarks first to decide if a bottleneck is worth pursuing, then use the hot
   * benchmarks to fine tune optimization efforts.
   *
   * The cold benchmark simulates a hot thread that enqueues requests, more than one
   * cold response threads consuming from the queue and an expiring thread that sweeps
   * the queue and removes already timed-out requests. This setup properly takes
   * into account the effects of holding on to the buffer segments during the think
   * time. Every thread group uses its own request queue so that we can avoid high
   * contention on the queue with large number of cores; the benchmark should measure
   * buffer operations rather than request queueing mechanics.
   *
   * We simulate think time for each benchmark thread by parking the thread for a
   * configurable number of microseconds (1000 by default).
   */


  @Benchmark
  @Threads(1)
  public void threads1hot(HotBuffers buffers) throws IOException {
    readWriteRecycle(buffers);
  }

  @Benchmark
  @Threads(2)
  public void threads2hot(HotBuffers buffers) throws IOException {
    readWriteRecycle(buffers);
  }

  @Benchmark
  @Threads(4)
  public void threads4hot(HotBuffers buffers) throws IOException {
    readWriteRecycle(buffers);
  }

  @Benchmark
  @Threads(8)
  public void threads8hot(HotBuffers buffers) throws IOException {
    readWriteRecycle(buffers);
  }

  @Benchmark
  @Threads(16)
  public void threads16hot(HotBuffers buffers) throws IOException {
    readWriteRecycle(buffers);
  }

  @Benchmark
  @Threads(32)
  public void threads32hot(HotBuffers buffers) throws IOException {
    readWriteRecycle(buffers);
  }

  private void readWriteRecycle(HotBuffers buffers) throws IOException {
    buffers.receive(requestBytes, buffers.process).readAll(NullSink);
    buffers.transmit(responseBytes, buffers.process).readAll(NullSink);
  }

  @Benchmark
  @GroupThreads(1)
  @Group("cold")
  public void readRequestHot(RequestHandler requests) throws IOException {
    requests.enqueueRequest(requestBytes);
  }

  @Benchmark
  @GroupThreads(6)
  @Group("cold")
  public void writeResponseCold(ResponseHandler responses) throws IOException {
    responses.processResponse(responseBytes).readAll(NullSink);
  }

  @Benchmark
  @GroupThreads(1)
  @Group("cold")
  public void sweepAndExpire(Sweeper sweeper) throws IOException {
    sweeper.sweepAndExpire(2 * maxThinkMicros, TimeUnit.MICROSECONDS);
  }

  static class Request {
    final Buffer data;
    final long receivedAt;

    Request(Buffer data, long receivedAt) {
      this.data = data;
      this.receivedAt = receivedAt;
    }
  }

  @State(Scope.Thread)
  public static class ResponseHandler extends BufferSetup {

    private RequestQueue requests;

    @Setup(Level.Iteration)
    public void setupQueue(RequestQueue requests) {
      super.bench = requests.bench;
      this.requests = requests;
    }

    private Request nextRequest() {
      int polls = 0;
      Request result;
      while((result = requests.queue.poll()) == null && ++polls < 10) {
        // We expect to spin rarely, if ever; requests are hot.
        Thread.yield();
      }
      assert result != null;
      requests.lazyAddAndGet(-1);
      return result;
    }

    BufferedSource processResponse(byte[] responseBytes) throws IOException {
      Request request = nextRequest();
      request.data.readAll(NullSink);
      return transmit(responseBytes, new Buffer());
    }

    @Setup(Level.Invocation)
    public void think() throws InterruptedException {
      TimeUnit.MICROSECONDS.sleep(bench.maxThinkMicros);
    }
  }

  @AuxCounters
  @State(Scope.Thread)
  public static class RequestHandler extends BufferSetup {

    private RequestQueue requests;

    /** JMH will include the fields below as secondary results.*/
    public long okResponses;
    public long goAwayResponses;

    @Setup(Level.Iteration)
    public void setupQueue(RequestQueue requests) {
      super.bench = requests.bench;
      this.requests = requests;
    }

    @Setup(Level.Iteration)
    public void resetCounters() {
      // See `AuxCounters` docs; we have to reset them manually
      // between iterations.
      okResponses = 0;
      goAwayResponses = 0;
    }

    boolean enqueueRequest(byte[] requestBytes) throws IOException {
      Buffer process = new Buffer();

      int currentCount = requests.count.get();
      if (currentCount < MaxRequests) {
        okResponses++;
        Request request = new Request(receive(requestBytes, process), System.nanoTime());

        requests.queue.add(request);
        return requests.lazyAddAndGet(1) < MaxRequests;
      }
      // If queue is full, we send a quick "go away" response.
      goAwayResponses++;
      transmit(requestBytes, process).readAll(NullSink);
      return false;
    }
  }

  @State(Scope.Group)
  public static class RequestQueue {
    /* A bounded queue for requests */
    final Queue<Request> queue = new ConcurrentLinkedQueue<>();
    /* Count of in-flight requests */
    final AtomicInteger count = new AtomicInteger();
    /* Bench parameters*/
    private BufferPerformanceBench bench;

    @Setup
    public void setupBench(BufferPerformanceBench bench) {
      this.bench = bench;
    }

    void sweepAndExpire(long timeOutNanos) throws IOException {
      Iterator<Request> index = queue.iterator();
      int expired = 0;
      long start = System.nanoTime();
      while(index.hasNext()) {
        Request request = index.next();
        long deadline = request.receivedAt + timeOutNanos;
        if (deadline >= start) {
          index.remove();
          expired++;
        }
      }
      count.addAndGet(-expired);
    }

    /**
     * We do not need memory effects of CAS for updating request counts;
     * the queue operations would take care of that if we depended on them
     * (which we don't).
     */
    int lazyAddAndGet(int delta) {
//      return count.addAndGet(delta);
      for (;;) {
        int current = count.get();
        int next = current + delta;
        if (count.weakCompareAndSet(current, next)) {
          return next;
        }
      }
    }
  }

  @State(Scope.Thread)
  public static class Sweeper {

    RequestQueue requests;

    @Setup(Level.Iteration)
    public void setupRequests(RequestQueue requests) {
      this.requests = requests;
    }

    void sweepAndExpire(long timeOut, TimeUnit unit) throws IOException {
      requests.sweepAndExpire(unit.toNanos(timeOut));
    }

    @Setup(Level.Invocation)
    public void sleep() throws InterruptedException {
      TimeUnit.MILLISECONDS.sleep(50L);
    }
  }

  @State(Scope.Thread)
  public static class HotBuffers extends BufferSetup {

    @SuppressWarnings("resource")
    final Buffer process = new Buffer();

    @Setup(Level.Trial)
    public void setupBench(BufferPerformanceBench bench) {
      super.bench = bench;
    }

    @TearDown(Level.Iteration)
    public void dispose() throws IOException {
      releaseBuffers(process);
    }

  }

  /*
   * The state class hierarchy is larger than it needs to be due to a JMH
   * issue where states inheriting setup methods depending on another state
   * do not get initialized correctly from benchmark methods making use
   * of groups. To work around, we leave the common setup and tear down code
   * in superclasses and move the setup method depending on the bench state
   * to subclasses. Without the workaround, it would have been enough for
   * `ColdBuffers` to inherit from `HotBuffers`.
   */

  @State(Scope.Thread)
  public static abstract class BufferSetup extends BufferState {
    BufferPerformanceBench bench;

    public Buffer receive(byte[] bytes, Buffer process) throws IOException {
      return super.receive(bytes, process, bench.maxReadBytes);
    }

    public Buffer transmit(byte[] bytes, Buffer process) throws IOException {
      return super.transmit(bytes, process, bench.maxWriteBytes);
    }

  }

  static abstract class BufferState {

    @SuppressWarnings("resource")
    final Buffer received = new Buffer();
    @SuppressWarnings("resource")
    final Buffer sent = new Buffer();

    public void releaseBuffers(Buffer... more) throws IOException {
      received.clear();
      sent.clear();
      for (Buffer buffer : more) {
        buffer.clear();
      }
    }

    /**
     * Fills up the receive buffer, hands off to process buffer and returns it for consuming.
     * Expects receive and process buffers to be empty. Leaves the receive buffer empty and
     * process buffer full.
     */
    protected Buffer receive(byte[] bytes, Buffer process, int maxChunkSize) throws IOException {
      writeChunked(received, bytes, maxChunkSize).readAll(process);
      return process;
    }

    /**
     * Fills up the process buffer, hands off to send buffer and returns it for consuming.
     * Expects process and sent buffers to be empty. Leaves the process buffer empty and
     * sent buffer full.
     */
    protected Buffer transmit(byte[] bytes, Buffer process, int maxChunkSize) throws IOException {
      writeChunked(process, bytes, maxChunkSize).readAll(sent);
      return sent;
    }

    private Buffer writeChunked(Buffer buffer, byte[] bytes, final int chunkSize) {
      int remaining = bytes.length;
      int offset = 0;
      while (remaining > 0) {
        int bytesToWrite = Math.min(remaining, chunkSize);
        buffer.write(bytes, offset, bytesToWrite);
        remaining -= bytesToWrite;
        offset += bytesToWrite;
      }
      return buffer;
    }

  }

  @SuppressWarnings("resource")
  private static final Sink NullSink = new Sink() {

    @Override public void write(Buffer source, long byteCount) throws EOFException {
      source.skip(byteCount);
    }

    @Override public void flush() {
      // nothing
    }

    @Override public Timeout timeout() {
      return Timeout.NONE;
    }

    @Override public void close() {
      // nothing
    }

    @Override public String toString() {
      return "NullSink{}";
    }
  };

  private byte[] storeSourceData(byte[] dest) throws IOException {
    requireNonNull(dest, "dest == null");

    try (BufferedSource source = Okio.buffer(Okio.source(OriginPath))) {
      source.readFully(dest);
    }
    return dest;
  }

  private void checkOrigin(File path) throws IOException {
    requireNonNull(path, "path == null");

    if (!path.canRead()) {
      throw new IllegalArgumentException("can not access: " + path);
    }

    try (InputStream in = new FileInputStream(path)) {
      int available = in.read();
      if (available < 0) {
        throw new IllegalArgumentException("can not read: " + path);
      }
    }
  }

}
