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

package org.apache.kafka.streams;

import dev.responsive.kafka.store.CassandraClientStub;
import java.time.Duration;
import java.time.Instant;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;

public class ResponsiveTestInputTopic<K, V> extends TestInputTopic<K, V> {

  private final CassandraClientStub client;

  public ResponsiveTestInputTopic(
      final CassandraClientStub client,
      final TopologyTestDriver driver,
      final String topicName,
      final Serializer<K> keySerializer,
      final Serializer<V> valueSerializer,
      final Instant startTimestamp,
      final Duration autoAdvance
  ) {
    super(driver, topicName, keySerializer, valueSerializer, startTimestamp, autoAdvance);
    this.client = client;
  }

  private void flush() {
    client.storeRegistry().stores().forEach(s -> s.getOnCommit().accept(0L));
  }
}
