/*
 * Copyright 2024 Responsive Computing, Inc.
 *
 * This source code is licensed under the Responsive Business Source License Agreement v1.0
 * available at:
 *
 * https://www.responsive.dev/legal/responsive-bsl-10
 *
 * This software requires a valid Commercial License Key for production use. Trial and commercial
 * licenses can be obtained at https://www.responsive.dev
 */

package dev.responsive.examples.common;

import org.apache.kafka.common.serialization.Deserializer;

public class JsonDeserializer<T> implements Deserializer<T> {

  private final Deserializer<T> delegate;

  @SuppressWarnings("resource")
  public JsonDeserializer(final Class<T> clazz) {
    this.delegate = new JsonSerde<>(clazz).deserializer();
  }

  @Override
  public T deserialize(final String topic, final byte[] data) {
    return delegate.deserialize(topic, data);
  }

}
