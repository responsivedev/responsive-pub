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