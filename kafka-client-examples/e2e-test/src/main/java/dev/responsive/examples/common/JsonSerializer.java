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

import org.apache.kafka.common.serialization.Serializer;

public class JsonSerializer<T> implements Serializer<T> {

  private final Serializer<T> delegate;

  @SuppressWarnings("resource")
  public JsonSerializer(Class<T> clazz) {
    this.delegate = new JsonSerde<>(clazz).serializer();
  }

  @Override
  public byte[] serialize(final String topic, final T data) {
    return delegate.serialize(topic, data);
  }
}
