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

package dev.responsive.kafka.api.async;

import static dev.responsive.kafka.internal.async.AsyncProcessor.createAsyncProcessor;
import static dev.responsive.kafka.internal.async.AsyncUtils.initializeAsyncBuilders;

import dev.responsive.kafka.internal.async.AsyncProcessor;
import dev.responsive.kafka.internal.async.stores.AbstractAsyncStoreBuilder;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.ConnectedStoreProvider;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.WrappedProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;

/**
 * A fixed-key version of the async processing api, for topologies that use the
 * fixed-key PAPI. This should wrap a regular processing and be substituted in its place
 * in the topology.
 */
public final class AsyncProcessorSupplier<KIn, VIn, KOut, VOut>
    implements WrappedProcessorSupplier<KIn, VIn, KOut, VOut> {

  private final ProcessorSupplier<KIn, VIn, KOut, VOut> userProcessorSupplier;

  private Map<String, AbstractAsyncStoreBuilder<?>> asyncStoreBuilders = null;

  public AsyncProcessorSupplier(
      final ProcessorSupplier<KIn, VIn, KOut, VOut> userProcessorSupplier
  ) {
    this.userProcessorSupplier = userProcessorSupplier;
  }

  @Override
  public Processor<KIn, VIn, KOut, VOut> get() {
    maybeInitializeAsyncStoreBuilders();
    return createAsyncProcessor(userProcessorSupplier.get(), asyncStoreBuilders);
  }

  @Override
  public Set<StoreBuilder<?>> stores() {
    maybeInitializeAsyncStoreBuilders();
    return new HashSet<>(asyncStoreBuilders.values());
  }

  private void maybeInitializeAsyncStoreBuilders() {
    if (asyncStoreBuilders == null) {
      asyncStoreBuilders = initializeAsyncBuilders(userProcessorSupplier.stores());
    }
  }
}
