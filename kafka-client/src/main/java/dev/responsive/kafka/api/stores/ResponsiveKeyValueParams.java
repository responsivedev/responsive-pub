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

package dev.responsive.kafka.api.stores;

import dev.responsive.kafka.internal.stores.SchemaTypes.KVSchema;
import dev.responsive.kafka.internal.utils.TableName;
import java.time.Duration;
import java.util.Optional;

public final class ResponsiveKeyValueParams {

  private final TableName name;
  private final KVSchema schema;

  private Optional<TtlProvider<?, ?>> ttlProvider = Optional.empty();

  private ResponsiveKeyValueParams(
      final String name,
      final KVSchema schema
  ) {
    this.name = new TableName(name);
    this.schema = schema;
  }

  public static ResponsiveKeyValueParams keyValue(final String name) {
    return new ResponsiveKeyValueParams(name, KVSchema.KEY_VALUE);
  }

  public static ResponsiveKeyValueParams fact(final String name) {
    return new ResponsiveKeyValueParams(name, KVSchema.FACT);
  }

  public ResponsiveKeyValueParams withTimeToLive(final Duration timeToLive) {
    return withTtlProvider(TtlProvider.withDefault(timeToLive));
  }

  public ResponsiveKeyValueParams withTtlProvider(final TtlProvider<?, ?> ttlProvider) {
    // If ttl is constant and infinite, it's equivalent to having no ttl at all
    if (ttlProvider.hasDefaultOnly() && !ttlProvider.defaultTtl().isFinite()) {
      this.ttlProvider = Optional.empty();
    } else {
      this.ttlProvider = Optional.of(ttlProvider);
    }
    return this;
  }

  public TableName name() {
    return name;
  }

  public KVSchema schemaType() {
    return schema;
  }

  public Optional<TtlProvider<?, ?>> ttlProvider() {
    return ttlProvider;
  }

}
