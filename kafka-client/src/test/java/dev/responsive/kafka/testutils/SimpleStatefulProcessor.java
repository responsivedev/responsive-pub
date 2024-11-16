/*
 * Copyright 2024 Responsive Computing, Inc.
 *
 * This source code is licensed under the Responsive Business Source License Agreement v1.0
 * available at:
 *
 * https://www.responsive.dev/legal/responsive-bsl-10
 *
 * This software requires a valid Commercial License Key for production use. Trial and commercial
 * licenses can be obtained at https://www.responsive.dev
 */

package dev.responsive.kafka.testutils;

import dev.responsive.kafka.testutils.SimpleStatefulProcessorSupplier.SimpleProcessorOutput;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
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
public class SimpleStatefulProcessor<VIn, VStored, VOut> implements FixedKeyProcessor<String, VIn, VOut> {

  private final Logger log = LoggerFactory.getLogger(SimpleStatefulProcessor.class);

  private final ComputeStatefulOutput<VIn, VStored, VOut> computeStatefulOutput;

  private final AtomicInteger processed;
  private final Map<String, VOut> latestValues;
  private final CountDownLatch processingLatch;

  private final String storeName;
  private final String streamThreadName;
  private int partition;

  private FixedKeyProcessorContext<String, VOut> context;
  private TimestampedKeyValueStore<String, VStored> kvStore;

  @FunctionalInterface
  public interface ComputeStatefulOutput<VIn, VStored, VOut> {
    SimpleProcessorOutput<VStored, VOut> computeOutput(
        ValueAndTimestamp<VStored> storedValue,
        FixedKeyRecord<String, VIn> inputRecord,
        FixedKeyProcessorContext<String, VOut> context
    );
  }

  public SimpleStatefulProcessor(
      final ComputeStatefulOutput<VIn, VStored, VOut> computeStatefulOutput,
      final String storeName,
      final AtomicInteger processed,
      final Map<String, VOut> latestValues,
      final CountDownLatch processingLatch
  ) {
    this.computeStatefulOutput = computeStatefulOutput;
    this.storeName = storeName;
    this.processed = processed;
    this.latestValues = latestValues;
    this.processingLatch = processingLatch;
    this.streamThreadName = Thread.currentThread().getName();
  }

  @Override
  public void init(final FixedKeyProcessorContext<String, VOut> context) {
    this.context = context;
    this.kvStore = context.getStateStore(storeName);
    this.partition = context.taskId().partition();

    log.debug("stream-thread [{}][{}] Initialized processor with store {}",
              streamThreadName, partition, storeName);
  }

  @Override
  public void process(final FixedKeyRecord<String, VIn> record) {
    log.debug("stream-thread [{}][{}] Processing input record: <{}, {}>",
              streamThreadName, partition, record.key(), record.value());

    final ValueAndTimestamp<VStored> oldValAndTimestamp = kvStore.get(record.key());

    final SimpleProcessorOutput<VStored, VOut> output = computeStatefulOutput.computeOutput(
        oldValAndTimestamp, record, context);

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
