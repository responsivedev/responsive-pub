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

package dev.responsive.kafka.internal.stores;

import dev.responsive.kafka.api.stores.TtlProvider;
import dev.responsive.kafka.api.stores.TtlProvider.TtlDuration;
import dev.responsive.kafka.internal.utils.StateDeserializer;
import java.util.Optional;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.StateSerdes;

public class TtlResolver<K, V> {

  public static final Optional<TtlResolver<?, ?>> NO_TTL = Optional.empty();

  private final StateDeserializer<K, V> stateDeserializer;
  private final TtlProvider<K, V> ttlProvider;

  @SuppressWarnings("unchecked")
  public static <K, V> Optional<TtlResolver<?, ?>> fromTtlProviderAndStateSerdes(
      final StateSerdes<?, ?> stateSerdes,
      final Optional<TtlProvider<?, ?>> ttlProvider
  ) {
    return ttlProvider.isPresent()
        ? Optional.of(
            new TtlResolver<>(
                (StateDeserializer<K, V>) new StateDeserializer<>(
                    stateSerdes.topic(),
                    stateSerdes.keyDeserializer(),
                    stateSerdes.valueDeserializer()),
                (TtlProvider<K, V>) ttlProvider.get()
            ))
        : Optional.empty();
  }

  public TtlResolver(
      final StateDeserializer<K, V> stateDeserializer,
      final TtlProvider<K, V> ttlProvider
  ) {
    this.stateDeserializer = stateDeserializer;
    this.ttlProvider = ttlProvider;
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
   * @return the raw result from the user's ttl computation function for this row
   */
  public Optional<TtlDuration> computeTtl(final Bytes keyBytes, final byte[] valueBytes) {
    return ttlProvider.computeTtl(keyBytes.get(), valueBytes, stateDeserializer);
  }

  /**
   * @return the actual ttl for this row after resolving the raw result returned by the user
   *         (eg applying the default value)
   */
  public TtlDuration resolveTtl(final Bytes keyBytes, final byte[] valueBytes) {
    final Optional<TtlDuration> ttl = computeTtl(keyBytes, valueBytes);
    return ttl.orElse(defaultTtl());
  }

}
