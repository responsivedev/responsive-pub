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

import static dev.responsive.kafka.api.async.internals.AsyncProcessor.createAsyncFixedKeyProcessor;
import static dev.responsive.kafka.api.async.internals.AsyncUtils.initializeAsyncBuilders;

import dev.responsive.kafka.api.async.internals.AsyncProcessor;
import dev.responsive.kafka.api.async.internals.stores.AbstractAsyncStoreBuilder;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.state.StoreBuilder;

/**
 * A fixed-key version of the async processing api, for topologies that use the
 * fixed-key PAPI. This should be used in place of
 * {@link FixedKeyProcessorSupplier} from Kafka Streams.
 * <p>
 * See javadocs for {@link AsyncProcessorSupplier} for full instructions and
 * documentation on the async processing framework.
 */
public class AsyncFixedKeyProcessorSupplier<KIn, VIn, VOut>
    implements FixedKeyProcessorSupplier<KIn, VIn, VOut> {

  private final FixedKeyProcessorSupplier<KIn, VIn, VOut> userProcessorSupplier;
  private final Map<String, AbstractAsyncStoreBuilder<?>> asyncStoreBuilders;

  /**
   * Create an AsyncProcessorSupplier that wraps a custom {@link ProcessorSupplier}
   * to enable async processing. If you aren't using a fixed-key processor, use
   * {@link AsyncProcessorSupplier#createAsyncProcessorSupplier} instead
   *
   * @param processorSupplier the {@link FixedKeyProcessorSupplier} that returns a (new)
   *                          instance of your custom {@link FixedKeyProcessor} on each
   *                          invocation of {@link ProcessorSupplier#get}
   */
  @SuppressWarnings("checkstyle:linelength")
  public static <KIn, VIn, VOut> AsyncFixedKeyProcessorSupplier<KIn, VIn, VOut> createAsyncProcessorSupplier(
      final FixedKeyProcessorSupplier<KIn, VIn, VOut> processorSupplier
  ) {
    return new AsyncFixedKeyProcessorSupplier<>(processorSupplier, processorSupplier.stores());
  }

  private AsyncFixedKeyProcessorSupplier(
      final FixedKeyProcessorSupplier<KIn, VIn, VOut> userProcessorSupplier,
      final Set<StoreBuilder<?>> userStoreBuilders
  ) {
    this.userProcessorSupplier = userProcessorSupplier;
    this.asyncStoreBuilders = initializeAsyncBuilders(userStoreBuilders);
  }

  @Override
  public AsyncProcessor<KIn, VIn, KIn, VOut> get() {
    return createAsyncFixedKeyProcessor(userProcessorSupplier.get(), asyncStoreBuilders);
  }

  @Override
  public Set<StoreBuilder<?>> stores() {
    return new HashSet<>(asyncStoreBuilders.values());
  }
}
