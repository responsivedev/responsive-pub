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

import static dev.responsive.kafka.internal.async.AsyncProcessor.createAsyncFixedKeyProcessor;
import static dev.responsive.kafka.internal.async.AsyncUtils.initializeAsyncBuilders;
import static dev.responsive.kafka.internal.utils.Utils.isExecutingOnStreamThread;

import dev.responsive.kafka.internal.async.AsyncProcessor;
import dev.responsive.kafka.internal.async.stores.AbstractAsyncStoreBuilder;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.WrappedFixedKeyProcessorSupplier;
import org.apache.kafka.streams.state.StoreBuilder;

/**
 * A fixed-key version of the async processing api, for topologies that use the
 * fixed-key PAPI. This should wrap a regular processing and be substituted in its place
 * in the topology.
 */
public class AsyncFixedKeyProcessorSupplier<KIn, VIn, VOut>
    implements WrappedFixedKeyProcessorSupplier<KIn, VIn, VOut> {

  private final FixedKeyProcessorSupplier<KIn, VIn, VOut> userProcessorSupplier;
  private Map<String, AbstractAsyncStoreBuilder<?>> asyncStoreBuilders = null;

  public AsyncFixedKeyProcessorSupplier(
      final FixedKeyProcessorSupplier<KIn, VIn, VOut> userProcessorSupplier
  ) {
    this.userProcessorSupplier = userProcessorSupplier;
  }

  @Override
  public FixedKeyProcessor<KIn, VIn, VOut> get() {
    if (maybeInitializeAsyncStoreBuilders()) {
      return createAsyncFixedKeyProcessor(userProcessorSupplier.get(), asyncStoreBuilders);
    } else {
      return userProcessorSupplier.get();
    }
  }

  @Override
  public Set<StoreBuilder<?>> stores() {
    if (maybeInitializeAsyncStoreBuilders()) {
      return new HashSet<>(asyncStoreBuilders.values());
    } else {
      return userProcessorSupplier.stores();
    }
  }

  private boolean maybeInitializeAsyncStoreBuilders() {
    if (!isExecutingOnStreamThread()) {
      // short circuit and delay the actual initialization until we detect that an actual
      // StreamThread is calling this, since the application will actually run through the
      // entire topology and build everything once when it first starts up, then throws it away
      return false;
    }

    if (asyncStoreBuilders == null) {
      asyncStoreBuilders = initializeAsyncBuilders(userProcessorSupplier.stores());
    }

    return true;
  }
}
