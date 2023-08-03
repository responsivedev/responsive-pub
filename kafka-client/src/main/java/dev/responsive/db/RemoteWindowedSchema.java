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

package dev.responsive.db;

import dev.responsive.model.Stamped;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueIterator;

/**
 * This specifies additional functions required for reading from
 * a windowed store.
 *
 * @see RemoteSchema
 */
public interface RemoteWindowedSchema extends RemoteSchema<Stamped<Bytes>> {

  KeyValueIterator<Stamped<Bytes>, byte[]> fetch(
      String tableName,
      int partition,
      Bytes key,
      long timeFrom,
      long timeTo
  );

  KeyValueIterator<Stamped<Bytes>, byte[]> backFetch(
      String tableName,
      int partition,
      Bytes key,
      long timeFrom,
      long timeTo
  );

  KeyValueIterator<Stamped<Bytes>, byte[]> fetchRange(
      String tableName,
      int partition,
      Bytes fromKey,
      Bytes toKey,
      long timeFrom,
      long timeTo
  );

  KeyValueIterator<Stamped<Bytes>, byte[]> backFetchRange(
      String tableName,
      int partition,
      Bytes fromKey,
      Bytes toKey,
      long timeFrom,
      long timeTo
  );

  KeyValueIterator<Stamped<Bytes>, byte[]> fetchAll(
      String tableName,
      int partition,
      long timeFrom,
      long timeTo
  );

  KeyValueIterator<Stamped<Bytes>, byte[]> backFetchAll(
      String tableName,
      int partition,
      long timeFrom,
      long timeTo
  );
}
