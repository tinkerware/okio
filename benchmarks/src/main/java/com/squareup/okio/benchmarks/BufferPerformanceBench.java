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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Phaser;
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
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.ThreadParams;

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

  /** Maximum size of a request queue. */
  public static final int MaxRequests = 10000;

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

  @Setup
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
  @GroupThreads(2)
  @Group("cold")
  @OperationsPerInvocation(10)
  public void writeResponseCold(ResponseHandler responses) throws IOException {
    responses.processResponseBatch(responseBytes, NullSink);
  }

  @Benchmark
  @GroupThreads(1)
  @Group("cold")
  public void sweepAndExpire(Sweeper sweeper) throws IOException {
    sweeper.sweepAndExpireWithTimeOut(2 * maxThinkMicros, TimeUnit.MICROSECONDS);
  }

  static class Request {

    final Buffer data;
    final long receivedAt;

    Request(Buffer data, long receivedAt) {
      this.data = requireNonNull(data);
      this.receivedAt = receivedAt;
    }
  }

  @AuxCounters
  @State(Scope.Thread)
  public static class ResponseHandler extends BufferSetup {

    private RequestQueue requests;
    private Queue<Request> queue;
    private ArrayList<Request> batch;
    private int batchSize;

    public long okResponses;

    @Setup(Level.Iteration)
    public void setupQueue(RequestQueue requests,
                           BenchmarkParams benchParams,
                           ThreadParams threadParams) throws InterruptedException {
      super.bench = requests.bench;
      this.requests = requests;
      this.batchSize = benchParams.getOpsPerInvocation();
      this.batch = new ArrayList<>(batchSize);
      this.queue = requests.ensureQueue(threadParams.getSubgroupThreadIndex());
      this.okResponses = 0;
    }

    private List<Request> nextBatch() {
      batch.clear();
      int polls = 0;

      nextPoll:
      while (polls++ < batchSize) {
        Request request;
        while ((request = queue.poll()) == null) {
          // We expect to spin rarely, if ever; requests are hot.
          Thread.yield();
          continue nextPoll;
        }
        batch.add(request);
      }
      requests.lazyAddAndGet(-batch.size());
      return batch;
    }

    void processResponseBatch(byte[] responseBytes, Sink sink) throws IOException {
      List<Request> batch = nextBatch();
      for (Request request : batch) {
        request.data.readAll(NullSink);
        transmit(responseBytes, new Buffer()).readAll(sink);
        okResponses++;
      }
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
    private int queueIndex;
    private int queueCount;

    public long goAwayResponses;

    @Setup(Level.Iteration)
    public void setupQueue(RequestQueue requests, BenchmarkParams benchParams) {
      super.bench = requests.bench;
      this.requests = requests;
      this.goAwayResponses = 0;
      this.queueCount = maxOf(benchParams.getThreadGroups());
      requests.awaitQueuesReady(queueCount);
    }

    private int maxOf(int... values) {
      int max = 0;
      for (int value : values) {
        max = Math.max(max, value);
      }
      return max;
    }

    public long expires() {
      return requests.expireCount;
    }

    boolean enqueueRequest(byte[] requestBytes) throws IOException {
      Buffer process = new Buffer();

      int currentCount = requests.count.get();
      if (currentCount < MaxRequests) {
        Request request = new Request(receive(requestBytes, process), System.nanoTime());

        currentQueue().add(request);
        return requests.lazyAddAndGet(1) < MaxRequests;
      }
      // If queue is full, we send a quick "go away" response.
      goAwayResponses++;
      transmit(requestBytes, process).readAll(NullSink);
      return false;
    }

    private Queue<Request> currentQueue() {
      // Spray requests over all response handler queues.
      Queue<Request> queue = requests.queues.get(queueIndex++);
      queueIndex %= queueCount;
      return queue;
    }
  }

  @State(Scope.Group)
  public static class RequestQueue {

    // Count of in-flight requests.
    final AtomicInteger count = new AtomicInteger();
    // Total count of expires performed in an iteration;
    // written by expire thread only and volatile to ensure
    // counter reads observe latest write.
    volatile long expireCount;
    // Bench parameters.
    private BufferPerformanceBench bench;
    // One queue per response handler.
    private final CopyOnWriteArrayList<Queue<Request>> queues =
        new CopyOnWriteArrayList<>(Collections.<Queue<Request>>nCopies(16, null));

    /*
     * We have to play a game of lazy initialization between the request and
     * response handlers, with the request queue mediating between them as
     * the coordinator since it is known to both. The request handler uses
     * `requestHandlerReady` to get notified when all response handlers
     * finish setting up their queues. In turn, the response handlers signal
     * completion of their queue setup through `responseHandlersReady`.
     * The parent-child relationship between the two phasers ensures that
     * when all response handlers deregister, the child phaser terminates
     * and deregisters itself from the parent phaser.
     */
    private final Phaser requestHandlerReady = new Phaser(1);
    private final Phaser responseHandlersReady = new Phaser(requestHandlerReady) {
      @Override protected boolean onAdvance(int phase, int registeredParties) {
        return false;
      }
    };

    @Setup(Level.Trial)
    public void setupBench(BufferPerformanceBench bench) {
      this.bench = bench;
    }

    @Setup(Level.Iteration)
    public void resetCounters() {
      expireCount = 0;
    }

    /**
     * Ensures that the given number of queues exist for a request handler's use,
     * waiting for response handlers to call `ensureQueue` as needed before returning
     * to the request handler.
     */
    void awaitQueuesReady(int queueCount) {
      responseHandlersReady.bulkRegister(queueCount);
      requestHandlerReady.awaitAdvance(requestHandlerReady.arrive());
    }

    /**
     * Ensures that a queue exists for the given queue index and returns it to a response handler.
     * We resort to this kind of lazy initialization for request queues due to a JMH issue in
     * injecting benchmark parameters to a group state that does not have the thread scope.
     */
    Queue<Request> ensureQueue(int queueIndex) throws InterruptedException {
      // Wait for request handler to arrive.
      while (responseHandlersReady.getRegisteredParties() == 0) {
        // Let the response handler spin lazily until the request handler
        // registers the expected queue count.
        Thread.yield();
      }

      Queue<Request> result = queues.get(queueIndex);
      if (result == null) {
        result = new ConcurrentLinkedQueue<>();
        queues.set(queueIndex, result);
      }
      else {
        result.clear();
      }
      responseHandlersReady.arriveAndDeregister();
      return result;
    }

    void sweepAndExpire(long timeOutNanos) throws IOException {
      for (Queue<Request> queue : queues) {
        if (queue != null) {
          int expired = sweepQueue(queue, timeOutNanos);
          expireCount += expired;
          count.addAndGet(-expired);
        }
      }
    }

    private int sweepQueue(Queue<Request> queue, long timeOutNanos) {
      Iterator<Request> index = queue.iterator();
      int expired = 0;
      long start = System.nanoTime();
      while (index.hasNext()) {
        Request request = index.next();
        long deadline = request.receivedAt + timeOutNanos;
        if (deadline <= start) {
          index.remove();
          expired++;
        }
      }
      return expired;
    }

    /**
     * We do not need memory effects of CAS for updating request counts; the queue operations would
     * take care of that if we depended on them (which we don't).
     */
    int lazyAddAndGet(int delta) {
//      return count.addAndGet(delta);
      for (; ; ) {
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

    void sweepAndExpireWithTimeOut(long timeOut, TimeUnit unit) throws IOException {
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
     * Expects receive and process buffers to be empty. Leaves the receive buffer empty and process
     * buffer full.
     */
    protected Buffer receive(byte[] bytes, Buffer process, int maxChunkSize) throws IOException {
      writeChunked(received, bytes, maxChunkSize).readAll(process);
      return process;
    }

    /**
     * Fills up the process buffer, hands off to send buffer and returns it for consuming. Expects
     * process and sent buffers to be empty. Leaves the process buffer empty and sent buffer full.
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
