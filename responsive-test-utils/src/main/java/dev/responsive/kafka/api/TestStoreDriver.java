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

package dev.responsive.kafka.api;

import java.time.Duration;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowStore;

/**
 * For use in testing, this {@link StreamsStoreDriver} will simply create
 * in memory stores for all state stores.
 */
public class TestStoreDriver implements StreamsStoreDriver {

  private final KafkaClientSupplier delegateClientSupplier = new DefaultKafkaClientSupplier();

  @Override
  public KeyValueBytesStoreSupplier kv(final String name) {
    return Stores.inMemoryKeyValueStore(name);
  }

  @Override
  public KeyValueBytesStoreSupplier timestampedKv(final String name) {
    return Stores.inMemoryKeyValueStore(name);
  }

  @Override
  public WindowBytesStoreSupplier windowed(final String name, final long retentionMs,
      final long windowSize, final boolean retainDuplicates) {
    return Stores.inMemoryWindowStore(
        name,
        Duration.ofMillis(retentionMs),
        Duration.ofMillis(windowSize),
        retainDuplicates
    );
  }

  @Override
  public KeyValueBytesStoreSupplier globalKv(final String name) {
    return kv(name);
  }

  @Override
  public <K, V> Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized(final String name) {
    return Materialized.as(timestampedKv(name));
  }

  @Override
  public <K, V> Materialized<K, V, WindowStore<Bytes, byte[]>> materialized(final String name,
      final long retentionMs, final long windowSize, final boolean retainDuplicates) {
    return Materialized.as(windowed(name, retentionMs, windowSize, retainDuplicates));
  }

  @Override
  public <K, V> Materialized<K, V, KeyValueStore<Bytes, byte[]>> globalMaterialized(
      final String name) {
    return Materialized.as(globalKv(name));
  }

}
