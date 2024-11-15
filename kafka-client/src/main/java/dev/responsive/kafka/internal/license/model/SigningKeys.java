/*
 * Copyright 2024 Responsive Computing, Inc.
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

package dev.responsive.kafka.internal.license.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Objects;

public class SigningKeys {
  private final List<SigningKey> keys;

  @JsonCreator
  public SigningKeys(@JsonProperty("keys") final List<SigningKey> keys) {
    this.keys = Objects.requireNonNull(keys);
  }

  public SigningKey lookupKey(final String keyId) {
    return keys.stream()
        .filter(k -> k.keyId.equals(keyId))
        .findFirst()
        .orElseThrow(() -> new IllegalArgumentException("Key not found: " + keyId));
  }

  public enum KeyType {
    RSA_4096
  }

  public static class SigningKey {
    private final KeyType type;
    private final String keyId;
    private final String path;

    @JsonCreator
    public SigningKey(
        @JsonProperty("type") final KeyType type,
        @JsonProperty("keyId") final String keyId,
        @JsonProperty("path") final String path
    ) {
      this.type = type;
      this.keyId = keyId;
      this.path = path;
    }

    public KeyType type() {
      return type;
    }

    public String path() {
      return path;
    }
  }
}
