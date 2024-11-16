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

package dev.responsive.kafka.internal.stores;

import dev.responsive.kafka.api.stores.TtlProvider;
import dev.responsive.kafka.api.stores.TtlProvider.TtlDuration;
import dev.responsive.kafka.internal.utils.StateDeserializer;
import java.util.Optional;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.StateSerdes;

public class TtlResolver<K, V> {

  public static final Optional<TtlResolver<?, ?>> NO_TTL = Optional.empty();

  private final StateDeserializer<K, V> stateDeserializer;
  private final TtlProvider<K, V> ttlProvider;
  private final ProcessorContext processorContext;

  @SuppressWarnings("unchecked")
  public static <K, V> Optional<TtlResolver<?, ?>> fromTtlProviderAndStateSerdes(
      final StateSerdes<?, ?> stateSerdes,
      final Optional<TtlProvider<?, ?>> ttlProvider,
      final ProcessorContext processorContext
  ) {
    return ttlProvider.isPresent()
        ? Optional.of(
            new TtlResolver<>(
                (StateDeserializer<K, V>) new StateDeserializer<>(
                    stateSerdes.topic(),
                    stateSerdes.keyDeserializer(),
                    stateSerdes.valueDeserializer()),
                (TtlProvider<K, V>) ttlProvider.get(),
                processorContext
            ))
        : Optional.empty();
  }

  public TtlResolver(
      final StateDeserializer<K, V> stateDeserializer,
      final TtlProvider<K, V> ttlProvider,
      final ProcessorContext processorContext
  ) {
    this.stateDeserializer = stateDeserializer;
    this.ttlProvider = ttlProvider;
    this.processorContext = processorContext;
  }

  public TtlDuration defaultTtl() {
    return ttlProvider.defaultTtl();
  }

  public boolean hasDefaultOnly() {
    return ttlProvider.hasDefaultOnly();
  }

  public boolean needsValueToComputeTtl() {
    return ttlProvider.needsValueToComputeTtl();
  }

  /**
   * @return the raw result from the user's ttl computation function for this row,
   *         adjusted by the difference between current time and the record timestamp.
   *         Used for writes.
   */
  public Optional<TtlDuration> computeInsertTtl(
      final Bytes keyBytes,
      final byte[] valueBytes,
      final long timestampMs
  ) {
    return ttlProvider.computeTtl(keyBytes.get(), valueBytes, stateDeserializer)
        .map(ttl -> {
          if (ttl.isFinite()) {
            return ttl.minus(processorContext.currentSystemTimeMs() - timestampMs);
          } else {
            return ttl;
          }
        });
  }

  /**
   * @return the actual ttl for this row after resolving the raw result returned by the user
   *         (eg applying the default value). Used for reads
   */
  public TtlDuration resolveRowTtl(final Bytes keyBytes, final byte[] valueBytes) {
    final Optional<TtlDuration> ttl =
        ttlProvider.computeTtl(keyBytes.get(), valueBytes,  stateDeserializer);

    return ttl.orElse(defaultTtl());
  }

}
