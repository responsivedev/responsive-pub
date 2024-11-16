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
