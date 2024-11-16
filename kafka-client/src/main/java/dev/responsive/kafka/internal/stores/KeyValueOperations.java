/*
 * Copyright 2024 Responsive Computing, Inc.
 *
 * This source code is licensed under the Responsive Software License Agreement v1.0 available at
 *
 * https://www.responsive.dev/legal/responsive-bsl-10
 *
 * This software requires a valid Commercial License Key for production use. Trial and commercial
 * licenses can be obtained at https://www.responsive.dev/sdk/get-started.
 */

package dev.responsive.kafka.internal.stores;

import java.io.Closeable;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.processor.internals.RecordBatchingStateRestoreCallback;
import org.apache.kafka.streams.state.KeyValueIterator;

public interface KeyValueOperations extends Closeable, RecordBatchingStateRestoreCallback {

  void put(final Bytes key, final byte[] value);

  byte[] delete(final Bytes key);

  byte[] get(final Bytes key);

  KeyValueIterator<Bytes, byte[]> range(final Bytes from, final Bytes to);

  KeyValueIterator<Bytes, byte[]> reverseRange(final Bytes from, final Bytes to);

  KeyValueIterator<Bytes, byte[]> all();

  KeyValueIterator<Bytes, byte[]> reverseAll();

  long approximateNumEntries();

  @Override
  void close();
}
