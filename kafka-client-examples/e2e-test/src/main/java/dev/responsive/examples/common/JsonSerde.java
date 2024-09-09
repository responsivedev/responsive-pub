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

package dev.responsive.examples.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class JsonSerde<T> implements Serde<T> {
  private final ObjectMapper objectMapper = new ObjectMapper();
  private final Class<T> clazz;

  public JsonSerde(Class<T> clazz) {
    this.clazz = clazz;
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
