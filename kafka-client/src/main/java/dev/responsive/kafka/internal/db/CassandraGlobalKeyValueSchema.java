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

package dev.responsive.kafka.internal.db;

import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.compaction.CompactionStrategy;
import java.time.Duration;

/**
 * Global schemas share many properties with Fact Schemas in that we want
 * each key to be its own partition, and we don't need LWTs for EOS support.
 * The only place it differs is that we expect overwrites, and that the pattern
 * is read-heavy with few updates. Because of this, we use leveled compaction
 * strategy instead of time windowed.
 */
public class CassandraGlobalKeyValueSchema extends CassandraFactSchema {

  public CassandraGlobalKeyValueSchema(final CassandraClient client) {
    super(client);
  }

  @Override
  protected CompactionStrategy<?> compactionStrategy(final Duration compactionWindow) {
    return SchemaBuilder.leveledCompactionStrategy();
  }
}
