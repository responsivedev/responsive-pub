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

import java.util.Objects;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.utils.Bytes;

public class BytesKeySpec implements KeySpec<Bytes> {

  @Override
  public Bytes keyFromRecord(final ConsumerRecord<byte[], byte[]> record) {
    return Bytes.wrap(record.key());
  }

  @Override
  public Bytes bytes(final Bytes key) {
    return key;
  }

  @Override
  public int compare(final Bytes o1, final Bytes o2) {
    return Objects.compare(o1, o2, Bytes::compareTo);
  }
}
