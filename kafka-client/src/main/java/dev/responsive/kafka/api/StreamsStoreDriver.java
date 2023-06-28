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
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowStore;

/**
 * An interface that allows for easily wrapping different Kafka Streams
 * state store implementations and swapping them at ease.
 */
public interface StreamsStoreDriver {

  /**
   * Create a {@link KeyValueBytesStoreSupplier} for a Responsive state store.
   * <p>
   * If you want to create a {@link TimestampedKeyValueStore timestamped} or
   * global {@link KeyValueStore}, you should use {@link #timestampedKv(String)}
   * or {@link #globalKv(String)}, respectively, for your store supplier instead.
   *
   * @param name the state store name
   * @return a key value store supplier with the given name that can be used to build a
   *         key-value store that uses Responsive's storage for its backend
   */
  KeyValueBytesStoreSupplier kv(final String name);

  /**
   * Create a {@link KeyValueBytesStoreSupplier} for a timestamped Responsive state store.
   * <p>
   * If you want to create a global {@link KeyValueStore}, you should use
   * {@link #globalKv(String)} for your store supplier instead.
   *
   * @param name the state store name
   * @return a key value store supplier with the given name that can be used to build a
   *        key-(timestamp/value) store that uses Responsive's storage for its backend
   */
  KeyValueBytesStoreSupplier timestampedKv(final String name);

  /**
   * Create a {@link KeyValueBytesStoreSupplier} for a global Responsive state store.
   * <p>
   * If you want to create a {@link TimestampedKeyValueStore timestamped} KeyValueStore,
   * you should use {@link #timestampedKv(String)} for your store supplier instead.
   *
   * @param name the state store name
   * @return a {@link KeyValueBytesStoreSupplier} with the given name that can be
   *         used to build a global key-value store backed by Responsive's storage
   */
  KeyValueBytesStoreSupplier globalKv(final String name);

  /**
   * Create a {@link WindowBytesStoreSupplier} for a windowed Responsive state store.
   *
   * @param name the state store name
   * @param retentionMs the retention for each entry
   * @param windowSize the window size
   * @param retainDuplicates whether to retain duplicates
   * @return a window store supplier with the given name that can be used to build a
   *         window store that uses Responsive's storage for its backend
   */
  WindowBytesStoreSupplier windowed(
      final String name,
      final long retentionMs,
      final long windowSize,
      final boolean retainDuplicates
  );

  /**
   * @see #kv(String)
   */
  <K, V> Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized(
      final String name
  );

  /**
   * @see #globalKv(String)
   */
  <K, V> Materialized<K, V, KeyValueStore<Bytes, byte[]>> globalMaterialized(
      final String name
  );

  /**
   * @see #windowed(String, long, long, boolean)
   */
  <K, V> Materialized<K, V, WindowStore<Bytes, byte[]>> windowMaterialized(
      final String name,
      final long retentionMs,
      final long windowSize,
      final boolean retainDuplicates
  );

}
