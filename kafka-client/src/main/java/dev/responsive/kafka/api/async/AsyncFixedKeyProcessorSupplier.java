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

import dev.responsive.kafka.api.async.internals.stores.AsyncStoreBuilder;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;

/**
 * A fixed-key version of the async processing api, for topologies that use the
 * fixed-key PAPI. This should be used in place of
 * {@link FixedKeyProcessorSupplier} from Kafka Streams.
 * <p>
 * See javadocs for {@link AsyncProcessorSupplier} for full instructions and
 * documentation on the async processing framework.
 */
public class AsyncFixedKeyProcessorSupplier<KIn, VIn, VOut> implements FixedKeyProcessorSupplier<KIn, VIn, VOut> {

  private final FixedKeyProcessorSupplier<KIn, VIn, VOut> userProcessorSupplier;
  private final Map<String, AsyncStoreBuilder<?>> asyncStoreBuilders = new HashMap<>();

  /**
   * See {@link AsyncProcessorSupplier#AsyncProcessorSupplier(ProcessorSupplier)}
   */
  public AsyncFixedKeyProcessorSupplier(
      final FixedKeyProcessorSupplier<KIn, VIn, VOut> processorSupplier
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
}
