/*
 * Copyright 2024 Responsive Computing, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package dev.responsive.kafka.api.async.internals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.stream.Collectors;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO:
//  1) add an equivalent form for FixedKeyProcessorSupplier and the like
//  2) share single thread pool across all async processors per StreamThread
//  3) make thread pool size configurable
//  4) implement RejectedExecutionHandler
//  5) Does ScheduledThreadPoolExecutor even make sense to use here??
//  6) Should we reuse the same async context or is it ok to construct a new wrapper each #init?
public class AsyncProcessor<KIn, VIn, KOut, VOut> implements Processor<KIn, VIn, KOut, VOut> {

  private final Logger log = LoggerFactory.getLogger(AsyncProcessor.class);

  private final Processor<KIn, VIn, KOut, VOut> userProcessor;
  private final List<String> connectedStoreNames;

  private final AsyncRecordQueue<KIn, VIn> recordQueue = new AsyncRecordQueue<>();
  private final ScheduledExecutorService threadPool =
      new ScheduledThreadPoolExecutor(10, (AsyncThread::new));

  private AsyncProcessorContext<KOut, VOut> asyncContext;


  public AsyncProcessor(
      final Processor<KIn, VIn, KOut, VOut> userProcessor,
      final String... connectedStoreNames
  ) {
    this.userProcessor = userProcessor;
    this.connectedStoreNames = Arrays.stream(connectedStoreNames).collect(Collectors.toList());
  }

  @Override
  public void init(final ProcessorContext<KOut, VOut> context) {
    this.log = new LogContext(String.format("async-processor [%s-%d]", context.taskId().partition())).logger(AsyncProcessor.class);

    userProcessor.init(context);

    final Map<String, AsyncKeyValueStore<?, ?>> allAsyncStores = asyncContext.getAllAsyncStores();
    if (allAsyncStores.keySet().size() != connectedStoreNames.size()) {
      throw new IllegalStateException("Number of actual stores initialized by this processor does"
          + "not match the number of connected store names that were provided to the "
          + "AsyncProcessorSupplier");
    }
  }

  @Override
  public void process(final Record<KIn, VIn> record) {
    // add new record to queue
  }

  @Override
  public void close() {
    userProcessor.close();
  }
}
