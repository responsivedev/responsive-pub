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

import dev.responsive.kafka.api.stores.ResponsiveKeyValueParams;
import dev.responsive.kafka.api.stores.ResponsiveStores;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.ValueAndTimestamp;

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
public class SimpleStatefulProcessorSupplier
    implements FixedKeyProcessorSupplier<String, String, String> {

  /**
   * A simple container class for the outputs of a {@link SimpleStatefulProcessor},
   * specifically the value to be forwarded downstream and the value to be stored
   * in the local processor state store. These can be different or the same value.
   */
  public static class SimpleProcessorOutput {
    public final String forwardedValue;
    public final String storedValue;
    
    public SimpleProcessorOutput(final String forwardedValue, final String storedValue) {
      this.forwardedValue = forwardedValue;
      this.storedValue = storedValue;
    }

    public SimpleProcessorOutput(final String outputValue) {
      this(outputValue, outputValue);
    }
    
  }
  
  private final BiFunction<ValueAndTimestamp<String>, FixedKeyRecord<String, String>, SimpleProcessorOutput> computeOutput;
  
  private final AtomicInteger processed;
  private final Map<String, String> latestValues;
  
  private final String storeName;
  private final Set<StoreBuilder<?>> storeBuilders;

  /**
   * @param computeOutput a simple function that computes the output from the input record and old value
   * @param params        the params to use to build a Responsive timestamped key-value store
   */
  public SimpleStatefulProcessorSupplier(
      final BiFunction<ValueAndTimestamp<String>, FixedKeyRecord<String, String>, SimpleProcessorOutput> computeOutput,
      final ResponsiveKeyValueParams params
  ) {
    this(computeOutput, params, new AtomicInteger(), new HashMap<>());
  }

  /**
   * @param computeOutput a simple function that computes the output from the input record and old value
   * @param params        the params to use to build a Responsive timestamped key-value store
   * @param processed     optional counter that can be used to monitor number of input records
   */
  public SimpleStatefulProcessorSupplier(
      final BiFunction<ValueAndTimestamp<String>, FixedKeyRecord<String, String>, SimpleProcessorOutput> computeOutput,
      final ResponsiveKeyValueParams params,
      final AtomicInteger processed
  ) {
    this(computeOutput, params, processed, new ConcurrentHashMap<>());
  }

  /**
   * @param computeOutput a simple function that computes the output from the input record and old value
   * @param params        the params to use to build a Responsive timestamped key-value store
   * @param latestValues  optional map to track latest value computed for each key
   */
  public SimpleStatefulProcessorSupplier(
      final BiFunction<ValueAndTimestamp<String>, FixedKeyRecord<String, String>, SimpleProcessorOutput> computeOutput,
      final ResponsiveKeyValueParams params,
      final Map<String, String> latestValues
  ) {
    this(computeOutput, params, new AtomicInteger(), latestValues);
  }

  /**
   * @param computeOutput a simple function that computes the output from the input record and old value
   * @param params        the params to use to build a Responsive timestamped key-value store
   * @param processed     optional counter that can be used to monitor number of input records
   * @param latestValues  optional map to track latest value computed for each key
   */
  public SimpleStatefulProcessorSupplier(
      final BiFunction<ValueAndTimestamp<String>, FixedKeyRecord<String, String>, SimpleProcessorOutput> computeOutput,
      final ResponsiveKeyValueParams params,
      final AtomicInteger processed,
      final Map<String, String> latestValues
  ) {
    this.computeOutput = computeOutput;
    this.storeName = params.name().kafkaName();
    this.processed = processed;
    this.latestValues = latestValues;
    this.storeBuilders = Collections.singleton(ResponsiveStores.timestampedKeyValueStoreBuilder(
        ResponsiveStores.keyValueStore(params),
        Serdes.String(),
        Serdes.String()));
  }
  
  @Override
  public FixedKeyProcessor<String, String, String> get() {
    return new SimpleStatefulProcessor(
        computeOutput,
        storeName, 
        processed, 
        latestValues
    );
  }
  
  @Override
  public Set<StoreBuilder<?>> stores() {
    return storeBuilders;
  }
  
}
