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

import dev.responsive.kafka.api.stores.ResponsiveKeyValueParams;
import dev.responsive.kafka.api.stores.ResponsiveStores;
import dev.responsive.kafka.testutils.SimpleStatefulProcessor.ComputeStatefulOutput;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.apache.kafka.streams.state.StoreBuilder;

/**
 * A simple fixed-key, String-typed, stateful processor that appends
 * incoming records and stores this list of received records in a 
 * timestamped kv store.
 * <p>
 * Allows for passing in an optional counter to keep track of the number
 * of records processed and an optional map to track the latest value
 * computed per key.
 * <p>
 * See also: {@link SimpleStatefulProcessor}
 */
@SuppressWarnings("checkstyle:linelength")
public class SimpleStatefulProcessorSupplier<VIn, VStored, VOut>
    implements FixedKeyProcessorSupplier<String, VIn, VOut> {

  /**
   * A simple container class for the outputs of a {@link SimpleStatefulProcessor},
   * specifically the value to be forwarded downstream and the value to be stored
   * in the local processor state store. These can be different or the same value.
   */
  public static class SimpleProcessorOutput<VStored, VOut> {
    public final VOut forwardedValue;
    public final VStored storedValue;
    
    public SimpleProcessorOutput(final VOut forwardedValue, final VStored storedValue) {
      this.forwardedValue = forwardedValue;
      this.storedValue = storedValue;
    }
  }
  
  private final ComputeStatefulOutput<VIn, VStored, VOut> computeStatefulOutput;

  private final AtomicInteger processed;
  private final Map<String, VOut> latestValues;
  private final CountDownLatch processingLatch;
  
  private final String storeName;
  private final Set<StoreBuilder<?>> storeBuilders;

  /**
   * @param computeStatefulOutput a simple function that computes the output from the input record and old value
   * @param params        the params to use to build a Responsive timestamped key-value store
   */
  public SimpleStatefulProcessorSupplier(
      final ComputeStatefulOutput<VIn, VStored, VOut> computeStatefulOutput,
      final ResponsiveKeyValueParams params,
      final Serde<VStored> storedValueSerde
  ) {
    this(computeStatefulOutput, params, storedValueSerde, new AtomicInteger(), new HashMap<>(), null);
  }

  /**
   * @param computeStatefulOutput a simple function that computes the output from the input record and old value
   * @param params        the params to use to build a Responsive timestamped key-value store
   * @param processed     optional counter that can be used to monitor number of input records
   */
  public SimpleStatefulProcessorSupplier(
      final ComputeStatefulOutput<VIn, VStored, VOut> computeStatefulOutput,
      final ResponsiveKeyValueParams params,
      final Serde<VStored> storedValueSerde,
      final AtomicInteger processed
  ) {
    this(computeStatefulOutput, params, storedValueSerde, processed, new ConcurrentHashMap<>(), null);
  }

  /**
   * @param computeStatefulOutput   a simple function that computes the output from the input record and old value
   * @param params          the params to use to build a Responsive timestamped key-value store
   * @param latestValues    optional map to track latest value computed for each key
   * @param processingLatch optional latch that counts down on each invocation of process
   */
  public SimpleStatefulProcessorSupplier(
      final ComputeStatefulOutput<VIn, VStored, VOut> computeStatefulOutput,
      final ResponsiveKeyValueParams params,
      final Serde<VStored> storedValueSerde,
      final Map<String, VOut> latestValues,
      final CountDownLatch processingLatch
  ) {
    this(computeStatefulOutput, params, storedValueSerde, new AtomicInteger(), latestValues, processingLatch);
  }

  /**
   * @param computeStatefulOutput a simple function that computes the output from the input record and old value
   * @param params        the params to use to build a Responsive timestamped key-value store
   * @param processed     optional counter that can be used to monitor number of input records
   * @param latestValues  optional map to track latest value computed for each key
   */
  public SimpleStatefulProcessorSupplier(
      final ComputeStatefulOutput<VIn, VStored, VOut> computeStatefulOutput,
      final ResponsiveKeyValueParams params,
      final Serde<VStored> storedValueSerde,
      final AtomicInteger processed,
      final Map<String, VOut> latestValues,
      final CountDownLatch processingLatch
  ) {
    this.computeStatefulOutput = computeStatefulOutput;
    this.storeName = params.name().kafkaName();
    this.processed = processed;
    this.latestValues = latestValues;
    this.processingLatch = processingLatch;
    this.storeBuilders = Collections.singleton(ResponsiveStores.timestampedKeyValueStoreBuilder(
        ResponsiveStores.keyValueStore(params),
        Serdes.String(),
        storedValueSerde));
  }
  
  @Override
  public FixedKeyProcessor<String, VIn, VOut> get() {
    return new SimpleStatefulProcessor<>(
        computeStatefulOutput,
        storeName, 
        processed, 
        latestValues,
        processingLatch
    );
  }
  
  @Override
  public Set<StoreBuilder<?>> stores() {
    return storeBuilders;
  }
  
}
