/*
 * Copyright 2023 Responsive Computing, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dev.responsive.kafka.api.stores;

import dev.responsive.kafka.api.stores.TtlProvider.TtlDuration;
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

  public Optional<TtlDuration> defaultTimeToLive() {
    if (ttlProvider.isPresent()) {
      return Optional.ofNullable(ttlProvider.get().defaultTtl());

    } else {
      return Optional.empty();
    }
  }

}
