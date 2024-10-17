/*
 * Copyright 2024 Responsive Computing, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package dev.responsive.kafka.internal.utils;

import java.util.Optional;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.internals.ValueAndTimestampSerde;

public class StateDeserializer<K, V> {
  private final String changelogTopic;
  private final Deserializer<K> keyDeserializer;
  private final Deserializer<V> valueDeserializer;
  private final Optional<Deserializer<ValueAndTimestamp<V>>> timestampedValueDeserializer;

  public StateDeserializer(
      final boolean isTimestamped,
      final String changelogTopic,
      final Serde<K> keySerde,
      final Serde<V> valueSerde
  ) {
    this.changelogTopic = changelogTopic;
    this.keyDeserializer = keySerde.deserializer();
    this.valueDeserializer = valueSerde.deserializer();

    if (isTimestamped) {
      timestampedValueDeserializer =
          Optional.of(new ValueAndTimestampSerde<>(valueSerde).deserializer());
    } else {
      timestampedValueDeserializer = Optional.empty();
    }
  }

  public K keyFrom(final byte[] keyBytes) {
    return keyDeserializer.deserialize(changelogTopic, keyBytes);
  }

  public V valueFrom(final byte[] valueBytes) {
    if (timestampedValueDeserializer.isEmpty()) {
      return valueDeserializer.deserialize(changelogTopic, valueBytes);
    } else {
      return timestampedValueDeserializer.get().deserialize(changelogTopic, valueBytes).value();
    }
  }
}
