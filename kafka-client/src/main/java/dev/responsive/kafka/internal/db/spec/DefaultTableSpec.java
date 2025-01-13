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

package dev.responsive.kafka.internal.db.spec;

import com.datastax.oss.driver.api.querybuilder.schema.CreateTableWithOptions;
import dev.responsive.kafka.api.config.ResponsiveConfig;
import dev.responsive.kafka.internal.db.partitioning.TablePartitioner;
import dev.responsive.kafka.internal.stores.TtlResolver;
import java.util.Optional;

public class DefaultTableSpec implements RemoteTableSpec {

  private final String name;
  private final TablePartitioner<?, ?> partitioner;
  private final Optional<TtlResolver<?, ?>> ttlResolver;
  private final ResponsiveConfig config;

  public DefaultTableSpec(
      final String name,
      final TablePartitioner<?, ?> partitioner,
      final Optional<TtlResolver<?, ?>> ttlResolver,
      final ResponsiveConfig config
  ) {
    this.name = name;
    this.partitioner = partitioner;
    this.ttlResolver = ttlResolver;
    this.config = config;
  }

  @Override
  public String tableName() {
    return name;
  }

  @Override
  public TablePartitioner<?, ?> partitioner() {
    return partitioner;
  }

  @Override
  public Optional<TtlResolver<?, ?>> ttlResolver() {
    return ttlResolver;
  }

  @Override
  public CreateTableWithOptions applyDefaultOptions(final CreateTableWithOptions base) {
    return base;
  }

  @Override
  public ResponsiveConfig config() {
    return config;
  }
}
