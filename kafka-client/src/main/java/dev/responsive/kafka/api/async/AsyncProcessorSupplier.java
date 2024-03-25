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

package dev.responsive.kafka.api.async;

import static dev.responsive.kafka.api.async.internals.Utils.initializeAsyncBuilders;

import dev.responsive.kafka.api.async.internals.AsyncProcessor;
import dev.responsive.kafka.api.async.internals.stores.AsyncStoreBuilder;
import dev.responsive.kafka.internal.stores.ResponsiveStoreBuilder;
import dev.responsive.kafka.internal.stores.ResponsiveStoreBuilder.StoreType;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.ConnectedStoreProvider;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.internals.AsyncTimestampedKeyValueStoreBuilder;

/**
 * Instructions:
 * 1) Simply wrap your regular {@link ProcessorSupplier} in the async supplier by passing
 *    it into the {@link AsyncProcessorSupplier} constructor, and then pass in the
 *    AsyncProcessorSupplier instead of your ProcessorSupplier when defining the application
 *    topology. The async framework will take care of the rest, and no further code
 *    changes are required to enable async processing!
 *
 * <p>
 *
 * Requirements/Setup:
 * 1) To use state stores within an async processor, you must connect the state stores via
 *    automatic connection. In other words, you must have your ProcessorSupplier override the
 *    {@link ConnectedStoreProvider#stores()} method and supply your store builders there,
 *    rather than connecting them to the processor manually via APIs like
 *    {@link StreamsBuilder#addStateStore(StoreBuilder)} (DSL) and
 *    {@link Topology#addStateStore} (PAPI)
 * 2) As is the case with regular, non-async processors, it is strongly recommended to
 *    use only "safe forwarding" in your processor, as "unsafe forwarding" can break casuality
 *    and lead to unexpected results. In other words, you should avoid mutating the input record
 *    and forwarding the mutated record as the output. The "safe forwarding" method
 *    entails creating a new {@link org.apache.kafka.streams.processor.api.Record} object with
 *    the desired key, value, and timestamp, as well as making a copy of the input record's
 *    {@link Headers} if you use headers and need to modify them or do anything other than passing
 *    them as-is. Headers are inherently mutatable and do not protect their backing array by making
 *    copies of it in the constructor or anywhere else. Therefore, if you ever add, remove, or
 *    update the input record's Headers, you must protect them by making a copy of the backing
 *    array and then creating a new instance of {@link Headers} with the new clone of the array.
 *    See the {@link ProcessorContext} javadocs for more details and examples of safe vs
 *    unsafe forwarding techniques.
 * 3) It is required to initialize any state stores connected to this processor inside of its
 *    {@link Processor#init(ProcessorContext)} method. Attempting to call
 *    {@link ProcessorContext#getStateStore(String)} after #init, for example inside the
 *    {@link Processor#process} method instead, will result in an exception being thrown.
 *
 * <p>
 *
 * Current limitations:
 * 0) Does not support read-your-write semantics WITHIN a single invocation of #process -- in
 *    other words, a #get after a #put on the same key is not necessarily guaranteed to return
 *    the value that was just inserted with #put, or include the new record in the results of
 *    a range scan.
 *    Note that this is only true within an invocation of #process: not from one invocation
 *    of #process to another, ie between different input records. The async processing framework
 *    guarantees that all previous input records of the same key will be processed in full
 *    before a new record with that key is picked up for async execution. This means any #put
 *    calls made while processing an input record at offset N will be reflected in the results
 *    of any #get calls on the same key when processing any input record at offset N + 1 or
 *    beyond.
 * 1) Proceed with caution when using range queries or performing any operations that affect,
 *    or depend on, the results or state of input records associated with a different key
 *    than that of the record which was passed into the current iteration of #process.
 *    Cross-key range scans, for example, will not necessarily include all records with
 *    a lower offset that have a different key. These may behave in a non-deterministic fashion
 *    as input records are only guaranteed to be processed in offset order relative to other
 *    input records with the same key. Since by definition, records of different keys are
 *    processed in parallel during async processing, you may see unpredictable results when
 *    attempting any operation that affects/uses other keys (such as {@link KeyValueStore#range}
 * 2) *Not compatible with punctuators or input records that originate in upstream punctuators
 * 3) Key-Value stores only at this time: async window and session stores coming soon
 * 4) Cannot be used for global state stores
 * 5) Async processors with multiple state stores cannot have state stores of different types,
 *    ie they must all use the same type of keySerde and the same type of valueSerde (though
 *    the keySerde and valueSerde themselves can be any type and don't need to match each other)
 * 6) Async processing will not be compatible with the new/upcoming "shareable state stores"
 *    feature -- an async processor must be the sole owner of any state stores connected
 *    to it. You can still access these stores from outside the async processor by using IQ,
 *    you just cannot access them from another processor or app by "sharing" them.
 * 7) Only single stateful processors are supported at this time. In other words, you can only
 *    enable async processing for a single node in the topology, which must be expressed as a
 *    {@link Processor} and requires at least one state store be connected.
 */
public final class AsyncProcessorSupplier<KIn, VIn, KOut, VOut> implements ProcessorSupplier<KIn, VIn, KOut, VOut> {

  private final ProcessorSupplier<KIn, VIn, KOut, VOut> userProcessorSupplier;
  private final Map<String, AsyncStoreBuilder<?>> asyncStoreBuilders = new HashMap<>();

  /**
   * Create an async wrapper around your custom {@link ProcessorSupplier} to enable parallel
   * processing of long/blocking calls and state store accesses. All the usual requirements for
   * the underlying {@link ProcessorSupplier} remain, such as connecting state stores to the
   * processor in your topology, which you must do via the automatic process ie overriding the
   * {@link ConnectedStoreProvider#stores()} method in your {@link ProcessorSupplier}
   * implementation and supplying a {@link StoreBuilder} for each state store that will be
   * accessed by this processor using {@link ProcessorContext#getStateStore(String)}
   *
   * @param processorSupplier the {@link ProcessorSupplier} that returns a (new) instance
   *                          of your custom {@link Processor} on each invocation of
   *                          {@link ProcessorSupplier#get}
   */
  public AsyncProcessorSupplier(
      final ProcessorSupplier<KIn, VIn, KOut, VOut> processorSupplier
  ) {
    this.userProcessorSupplier = processorSupplier;
    this.asyncStoreBuilders.putAll(initializeAsyncBuilders(userProcessorSupplier.stores()));
  }

  @Override
  public AsyncProcessor<KIn, VIn, KOut, VOut> get() {
    return new AsyncProcessor<>(userProcessorSupplier.get(), asyncStoreBuilders);
  }

  @Override
  public Set<StoreBuilder<?>> stores() {
    return new HashSet<>(asyncStoreBuilders.values());
  }
}
