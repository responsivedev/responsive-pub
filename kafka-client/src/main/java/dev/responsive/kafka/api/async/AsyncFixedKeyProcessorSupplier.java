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

import static dev.responsive.kafka.api.async.internals.AsyncUtils.initializeAsyncBuilders;

import dev.responsive.kafka.api.async.internals.MaybeAsyncProcessor;
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
  private final Map<String, AbstractAsyncStoreBuilder<?, ?, ?>> asyncStoreBuilders;

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
    if (userStoreBuilders == null || userStoreBuilders.isEmpty()) {
      throw new UnsupportedOperationException(
          "Async processing currently requires at least one state store be "
              + "connected to the async processor, and that stores be connected "
              + "by implementing the #stores method in your processor supplier");
    }

    this.userProcessorSupplier = userProcessorSupplier;
    this.asyncStoreBuilders = initializeAsyncBuilders(userStoreBuilders);
  }

  @Override
  public MaybeAsyncProcessor<KIn, VIn, KIn, VOut> get() {
    return MaybeAsyncProcessor.createFixedKeyProcessor(userProcessorSupplier.get(), asyncStoreBuilders);
  }

  @Override
  public Set<StoreBuilder<?>> stores() {
    return new HashSet<>(asyncStoreBuilders.values());
  }
}
