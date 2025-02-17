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

public class UsageBasedV1 extends LicenseInfo {

  public static final String TYPE_NAME = "usage_based_v1";

  private final String email;
  private final String key;
  private final String secret;

  @JsonCreator
  public UsageBasedV1(
      @JsonProperty("type") final String type,
      @JsonProperty("email") final String email,
      @JsonProperty("key") final String key,
      @JsonProperty("secret") final String secret
  ) {
    super(type);
    this.email = email;
    this.key = key;
    this.secret = secret;
  }

  public String email() {
    return email;
  }

  public String key() {
    return key;
  }

  public String secret() {
    return secret;
  }
}
