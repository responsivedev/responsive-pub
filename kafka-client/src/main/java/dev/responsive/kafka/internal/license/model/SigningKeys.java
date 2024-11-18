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
