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

package dev.responsive.kafka.internal.stores;

import dev.responsive.kafka.api.config.ResponsiveConfig;
import dev.responsive.kafka.api.stores.ResponsiveKeyValueParams;
import dev.responsive.kafka.internal.db.partitioning.SubPartitioner;
import dev.responsive.kafka.internal.utils.SharedClients;
import dev.responsive.kafka.internal.utils.TableName;
import java.util.List;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;

public class ResponsiveFactStore extends ResponsivePartitionedStore {

  public ResponsiveFactStore(final ResponsiveKeyValueParams params) {
    super(params);
  }

  @Override
  protected SubPartitioner partitioner(
      final ResponsiveConfig config,
      final SharedClients clients,
      final TableName name,
      final TopicPartition partition
  ) {
    return SubPartitioner.NO_SUBPARTITIONS;
  }

  @Override
  public void put(final Bytes key, final byte[] value) {
    throw new UnsupportedOperationException("Fact stores only support putIfAbsent insertions.");
  }

  @Override
  public void putAll(final List<KeyValue<Bytes, byte[]>> entries) {
    throw new UnsupportedOperationException("Fact stores only support putIfAbsent insertions."
        + " Note that for Responsive stores there is no performance penalty for calling put "
        + "multiple times, the client will batch the requests internally.");
  }

  @Override
  public byte[] delete(final Bytes key) {
    throw new UnsupportedOperationException("Fact stores do not support explicit deletion. "
        + "Please use the timeToLive feature or a normal key-value store instead.");
  }
}
