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

package dev.responsive.kafka.internal.db.spec;

import com.datastax.oss.driver.api.querybuilder.schema.CreateTableWithOptions;
import com.datastax.oss.driver.api.querybuilder.schema.compaction.CompactionStrategy;
import com.datastax.oss.driver.internal.querybuilder.schema.compaction.DefaultLeveledCompactionStrategy;
import dev.responsive.kafka.internal.db.partitioning.TablePartitioner;
import dev.responsive.kafka.internal.stores.TtlResolver;
import java.util.Optional;

public class DefaultTableSpec implements RemoteTableSpec {

  private static final CompactionStrategy<?> DEFAULT_CASSANDRA_COMPACTION_STRATEGY =
      new DefaultLeveledCompactionStrategy();

  private final String name;
  private final TablePartitioner<?, ?> partitioner;
  private final Optional<TtlResolver<?, ?>> ttlResolver;

  public DefaultTableSpec(
      final String name,
      final TablePartitioner<?, ?> partitioner,
      final Optional<TtlResolver<?, ?>> ttlResolver
  ) {
    this.name = name;
    this.partitioner = partitioner;
    this.ttlResolver = ttlResolver;
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
    return base.withCompaction(DEFAULT_CASSANDRA_COMPACTION_STRATEGY);
  }
}
