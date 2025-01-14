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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.ProcessorWrapper;
import org.apache.kafka.streams.processor.api.WrappedFixedKeyProcessorSupplier;
import org.apache.kafka.streams.processor.api.WrappedProcessorSupplier;
import org.apache.kafka.streams.state.StoreBuilder;

// TODO(sophie): only wrap when stores are Responsive (rather than eg rocksdb or in-memory)
//  note: how to account for processors that access state via ValueGetters but don't actually own
//  the store themselves?
//  -- do they still implement #stores?
//  -- does the async framework need to account for reads from external processors?
//  also: test with not-directly connected stores (eg value getters or table joins which connect
//  to the upstream Ktable) where there is a repartition upstream as well
//  -- integration test failure (didn't read all events?)
//  -- try running topology for StreamsBuilderTests that failed when I removed
//     the StoreBuilderWrapper<->FactoryWrappingStoreBuilder unwrapping thing
//  -- wrap global stores and read-only stores?
public class AsyncProcessorWrapper implements ProcessorWrapper {

  @SuppressWarnings("checkstyle:linelength")
  public static final String ASYNC_PROCESSOR_WRAPPER_CONTEXT_CONFIG = "__internal.responsive.async.processor.wrapper.context__";

  // Effectively final after #configure
  private ProcessorWrapperContext wrapperContext;

  @Override
  public void configure(final Map<String, ?> configs) {
    final ProcessorWrapperContext configuredContext =
        (ProcessorWrapperContext) configs.get(ASYNC_PROCESSOR_WRAPPER_CONTEXT_CONFIG);

    if (configuredContext == null) {
      wrapperContext = new ProcessorWrapperContext();
    } else {
      wrapperContext = configuredContext;
    }
  }

  @Override
  public <KIn, VIn, KOut, VOut> WrappedProcessorSupplier<KIn, VIn, KOut, VOut> wrapProcessorSupplier(
      final String processorName,
      final ProcessorSupplier<KIn, VIn, KOut, VOut> processorSupplier
  ) {
    final var stores = processorSupplier.stores();

    wrapperContext.registerWrappedProcessor(processorName, stores);

    if (stores != null && !stores.isEmpty()) {
      return AsyncProcessorSupplier.createAsyncProcessorSupplier(processorSupplier);
    } else {
      return ProcessorWrapper.asWrapped(processorSupplier);
    }
  }

  @Override
  public <KIn, VIn, VOut> WrappedFixedKeyProcessorSupplier<KIn, VIn, VOut> wrapFixedKeyProcessorSupplier(
      final String processorName,
      final FixedKeyProcessorSupplier<KIn, VIn, VOut> processorSupplier
  ) {
    final var stores = processorSupplier.stores();

    wrapperContext.registerWrappedProcessor(processorName, stores);

    if (stores != null && !stores.isEmpty()) {
      return AsyncFixedKeyProcessorSupplier.createAsyncProcessorSupplier(processorSupplier);
    } else {
      return ProcessorWrapper.asWrappedFixedKey(processorSupplier);
    }

  }

  public static class ProcessorWrapperContext {
    private final Map<String, StoreBuilder<?>> wrappedStoreBuilders = new HashMap<>();
    private final Map<String, Set<String>> processorToWrappedStores = new HashMap<>();

    public void registerWrappedProcessor(final String processorName, final Set<StoreBuilder<?>> stores) {
      final Set<String> processorStores =
          processorToWrappedStores.computeIfAbsent(processorName, p -> new HashSet<>());

      if (stores != null) {
        for (final StoreBuilder<?> builder : stores) {
          final String storeName = builder.name();
          wrappedStoreBuilders.put(storeName, builder);
          processorStores.add(storeName);
        }
      }
    }

    public Set<String> allWrappedStoreNames() {
      return wrappedStoreBuilders.keySet();
    }

    public Set<String> allWrappedProcessorNames() {
      return processorToWrappedStores.keySet();
    }
  }
}
