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

package dev.responsive.kafka.testutils;

import dev.responsive.kafka.testutils.SimpleStatefulProcessorSupplier.SimpleProcessorOutput;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple fixed-key, string-typed, stateful processor that logs all records and actions
 * at DEBUG and has some built in optional utilities for observing results such as counting
 * the number of input records processed or storing the latest output values in a map.
 * <p>
 * See also: {@link SimpleStatefulProcessorSupplier}
 */
@SuppressWarnings("checkstyle:linelength")
public class SimpleStatefulProcessor implements FixedKeyProcessor<String, String, String> {

  private final Logger log = LoggerFactory.getLogger(SimpleStatefulProcessor.class);

  private final BiFunction<ValueAndTimestamp<String>, FixedKeyRecord<String, String>, SimpleProcessorOutput> computeOutput;

  private final AtomicInteger processed;
  private final Map<String, String> latestValues;
  private final CountDownLatch processingLatch;

  private final String storeName;
  private final String streamThreadName;
  private int partition;

  private FixedKeyProcessorContext<String, String> context;
  private TimestampedKeyValueStore<String, String> kvStore;

  public SimpleStatefulProcessor(
      final BiFunction<ValueAndTimestamp<String>, FixedKeyRecord<String, String>, SimpleProcessorOutput> computeOutput,
      final String storeName,
      final AtomicInteger processed,
      final Map<String, String> latestValues,
      final CountDownLatch processingLatch
  ) {
    this.computeOutput = computeOutput;
    this.storeName = storeName;
    this.processed = processed;
    this.latestValues = latestValues;
    this.processingLatch = processingLatch;
    this.streamThreadName = Thread.currentThread().getName();
  }

  @Override
  public void init(final FixedKeyProcessorContext<String, String> context) {
    this.context = context;
    this.kvStore = context.getStateStore(storeName);
    this.partition = context.taskId().partition();

    log.debug("stream-thread [{}][{}] Initialized processor with store {}",
              streamThreadName, partition, storeName);
  }

  @Override
  public void process(final FixedKeyRecord<String, String> record) {
    log.debug("stream-thread [{}][{}] Processing input record: <{}, {}>",
              streamThreadName, partition, record.key(), record.value());

    final ValueAndTimestamp<String> oldValAndTimestamp = kvStore.get(record.key());

    final SimpleProcessorOutput output = computeOutput.apply(oldValAndTimestamp, record);

    kvStore.put(record.key(), ValueAndTimestamp.make(output.storedValue, record.timestamp()));
    context.forward(record.withValue(output.forwardedValue));

    log.debug("stream-thread [{}][{}] Computed output: <{}, {}>",
              streamThreadName, partition, record.key(), output);

    processed.incrementAndGet();
    latestValues.put(record.key(), output.forwardedValue);
    if (processingLatch != null) {
      processingLatch.countDown();
    }
  }

  @Override
  public void close() {
    log.debug("stream-thread [{}][{}] Closed processor with store {}",
              streamThreadName, partition, storeName);
  }

}
