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

import dev.responsive.kafka.internal.utils.Stamped;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueIterator;

public interface RemoteWindowedTable<S> extends RemoteTable<Stamped, S> {

  byte[] fetch(
      int partition,
      Bytes key,
      long windowStart
  );

  KeyValueIterator<Stamped, byte[]> fetch(
      int partition,
      Bytes key,
      long timeFrom,
      long timeTo
  );

  KeyValueIterator<Stamped, byte[]> backFetch(
      int partition,
      Bytes key,
      long timeFrom,
      long timeTo
  );

  KeyValueIterator<Stamped, byte[]> fetchRange(
      int partition,
      Bytes fromKey,
      Bytes toKey,
      long timeFrom,
      long timeTo
  );

  KeyValueIterator<Stamped, byte[]> backFetchRange(
      int partition,
      Bytes fromKey,
      Bytes toKey,
      long timeFrom,
      long timeTo
  );

  KeyValueIterator<Stamped, byte[]> fetchAll(
      int partition,
      long timeFrom,
      long timeTo
  );

  KeyValueIterator<Stamped, byte[]> backFetchAll(
      int partition,
      long timeFrom,
      long timeTo
  );
}
