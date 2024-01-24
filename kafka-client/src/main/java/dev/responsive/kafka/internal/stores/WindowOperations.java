/*
 *
 *  * Copyright 2023 Responsive Computing, Inc.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package dev.responsive.kafka.internal.stores;

import java.io.Closeable;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.internals.RecordBatchingStateRestoreCallback;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.WindowStoreIterator;

public interface WindowOperations extends Closeable, RecordBatchingStateRestoreCallback {

  void put(final Bytes key, final byte[] value, final long windowStartTime);

  void delete(final Bytes key, final long windowStartTime);

  byte[] fetch(final Bytes key, final long windowStartTime);

  WindowStoreIterator<byte[]> fetch(
      final Bytes key,
      final long timeFrom,
      final long timeTo
  );

  KeyValueIterator<Windowed<Bytes>, byte[]> fetch(
      final Bytes keyFrom,
      final Bytes keyTo,
      final long timeFrom,
      final long timeTo
  );

  KeyValueIterator<Windowed<Bytes>, byte[]> fetchAll(
      final long timeFrom,
      final long timeTo
  );

  KeyValueIterator<Windowed<Bytes>, byte[]> all();

  WindowStoreIterator<byte[]> backwardFetch(
      final Bytes key,
      final long timeFrom,
      final long timeTo
  );

  KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetch(
      final Bytes keyFrom,
      final Bytes keyTo,
      final long timeFrom,
      final long timeTo
  );

  KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetchAll(
      final long timeFrom,
      final long timeTo
  );

  KeyValueIterator<Windowed<Bytes>, byte[]> backwardAll();

  /**
   * @return true if the latest window (ie windowStartMs == observedStreamTime) is "broken" up
   *  by the store having been closed and reopened, for example due to restarts and rebalances.
   *  If true, things tracked in memory such as the active window bloom filter cannot be covering
   *  the entire window and will be missing some elements, so they should not be trusted.
   */
  boolean brokenWindow();

  @Override
  void close();
}
