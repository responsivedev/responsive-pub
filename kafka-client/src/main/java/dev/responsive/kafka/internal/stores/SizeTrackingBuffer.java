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

package dev.responsive.kafka.internal.stores;

import dev.responsive.kafka.internal.utils.Result;
import java.util.Collection;
import org.apache.kafka.streams.state.KeyValueIterator;

public interface SizeTrackingBuffer<K extends Comparable<K>> {

  long sizeInBytes();

  int sizeInRecords();

  void put(final K key, final Result<K> value);

  void clear();

  Result<K> get(K key);

  KeyValueIterator<K, Result<K>> range(K from, K to);

  KeyValueIterator<K, Result<K>> reverseRange(K from, K to);

  KeyValueIterator<K, Result<K>> all();

  KeyValueIterator<K, Result<K>> reverseAll();

  Collection<Result<K>> values();
}