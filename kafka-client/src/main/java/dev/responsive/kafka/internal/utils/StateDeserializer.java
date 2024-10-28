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

import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StateDeserializer<K, V> {
  private static final Logger LOG = LoggerFactory.getLogger(StateDeserializer.class);

  private final String changelogTopic;
  private final Deserializer<K> keyDeserializer;
  private final Deserializer<V> valueDeserializer;

  public StateDeserializer(
      final String changelogTopic,
      final Deserializer<K> keyDeserializer,
      final Deserializer<V> valueDeserializer
  ) {
    this.changelogTopic = changelogTopic;
    this.keyDeserializer = keyDeserializer;
    this.valueDeserializer = valueDeserializer;
  }

  public K keyFrom(final byte[] keyBytes) {
    if (keyBytes == null) {
      final String errMgs = String.format("Tried to deserialize key that was not provided");
      LOG.error(errMgs);
      throw new IllegalStateException(errMgs);
    }

    return keyDeserializer.deserialize(changelogTopic, keyBytes);
  }

  public V valueFrom(final byte[] valueBytes) {
    if (valueBytes == null) {
      final String errMgs = String.format("Tried to deserialize value that was not provided");
      LOG.error(errMgs);
      throw new IllegalStateException(errMgs);
    }

    return valueDeserializer.deserialize(changelogTopic, valueBytes);
  }
}
