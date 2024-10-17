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

import dev.responsive.kafka.api.stores.ResponsiveKeyValueParams;
import dev.responsive.kafka.api.stores.ResponsiveWindowParams;
import dev.responsive.kafka.internal.db.partitioning.Segmenter.SegmentPartition;
import dev.responsive.kafka.internal.db.partitioning.TablePartitioner;
import dev.responsive.kafka.internal.db.spec.DefaultCassandraTableSpec;
import dev.responsive.kafka.internal.db.spec.CassandraTableSpec;
import dev.responsive.kafka.internal.stores.TtlResolver;
import dev.responsive.kafka.internal.utils.WindowedKey;
import org.apache.kafka.common.utils.Bytes;

/**
 * Translates {@link dev.responsive.kafka.api.stores.ResponsiveKeyValueParams}
 * and {@link dev.responsive.kafka.api.stores.ResponsiveWindowParams} into
 * corresponding {@link CassandraTableSpec} instances.
 *
 * <p>Do not move functionality from this class into the above classes since
 * those are public classes and it's better to keep this functionality
 * internal.</p>
 */
public class RemoteTableSpecFactory {

  public static CassandraTableSpec globalSpec(
      final ResponsiveKeyValueParams params,
      final TablePartitioner<Bytes, Integer> partitioner,
      final TtlResolver<?, ?> ttlResolver
  ) {
    return new DefaultCassandraTableSpec(
        params.name().tableName(),
        partitioner,
        ttlResolver
    );
  }

  public static CassandraTableSpec fromKVParams(
      final ResponsiveKeyValueParams params,
      final TablePartitioner<Bytes, Integer> partitioner,
      final TtlResolver<?, ?> ttlResolver
  ) {
    return new DefaultCassandraTableSpec(
        params.name().tableName(),
        partitioner,
        ttlResolver
    );
  }

  public static CassandraTableSpec fromWindowParams(
      final ResponsiveWindowParams params,
      final TablePartitioner<WindowedKey, SegmentPartition> partitioner
  ) {
    return new DefaultCassandraTableSpec(params.name().tableName(), partitioner, null);
  }

}
