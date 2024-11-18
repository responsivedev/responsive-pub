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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class JsonSerde<T> implements Serde<T> {
  private final ObjectMapper objectMapper = new ObjectMapper();
  private final Class<T> clazz;

  public JsonSerde(Class<T> clazz) {
    this.clazz = clazz;
    objectMapper.registerModule(new Jdk8Module());
  }

  @Override
  public Serializer<T> serializer() {
    return (topic, data) -> {
      try {
        return objectMapper.writeValueAsBytes(data);
      } catch (Exception e) {
        throw new RuntimeException("Failed to serialize object", e);
      }
    };
  }

  @Override
  public Deserializer<T> deserializer() {
    return (topic, bytes) -> {
      try {
        return objectMapper.readValue(bytes, clazz);
      } catch (Exception e) {
        throw new RuntimeException("Failed to deserialize object", e);
      }
    };
  }
}
