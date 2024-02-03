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

import dev.responsive.kafka.api.async.ResponsiveFuture;
import java.io.Closeable;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.processor.internals.RecordBatchingStateRestoreCallback;
import org.apache.kafka.streams.state.KeyValueIterator;

public interface KeyValueOperations extends Closeable, RecordBatchingStateRestoreCallback {

  void put(final Bytes key, final byte[] value);

  byte[] delete(final Bytes key);

  byte[] get(final Bytes key);
  
  ResponsiveFuture<byte[]> getAsync(final Bytes key);

  KeyValueIterator<Bytes, byte[]> range(final Bytes from, final Bytes to);

  KeyValueIterator<Bytes, byte[]> reverseRange(final Bytes from, final Bytes to);

  KeyValueIterator<Bytes, byte[]> all();

  KeyValueIterator<Bytes, byte[]> reverseAll();

  long approximateNumEntries();

  @Override
  void close();
}
