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

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowStore;

/**
 * An interface that allows for easily wrapping different Kafka Streams
 * state store implementations and swapping them at ease.
 */
public interface StreamsStoreDriver extends KafkaClientSupplier {

  /**
   * @param name the state store name
   * @return a key value store supplier with the given name that uses Responsive's
   *         storage for its backend
   */
  KeyValueBytesStoreSupplier kv(final String name);

  /**
   * @param name the state store name
   * @param retentionMs the retention for each entry
   * @param windowSize the window size
   * @param retainDuplicates whether to retain duplicates
   * @return a windowed key value store supplier with the given name that
   *         uses Responsive's storage for its backend
   */
  WindowBytesStoreSupplier windowed(
      final String name,
      final long retentionMs,
      final long windowSize,
      final boolean retainDuplicates
  );

  /**
   * Creates a materialization for a global store, which handles update and
   * read semantics differently from normal stores.
   *
   * @param name the state store name
   * @return a key value store supplier with the given name that uses Responsive's
   *         storage for its backend
   */
  KeyValueBytesStoreSupplier globalKv(final String name);

  /**
   * @see #kv(String)
   */
  <K, V> Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized(
      final String name
  );

  /**
   * @see #windowed(String, long, long, boolean)
   */
  <K, V> Materialized<K, V, WindowStore<Bytes, byte[]>> materialized(
      final String name,
      final long retentionMs,
      final long windowSize,
      final boolean retainDuplicates
  );

  /**
   * @see #globalKv(String)
   */
  <K, V> Materialized<K, V, KeyValueStore<Bytes, byte[]>> globalMaterialized(
      final String name
  );

}
