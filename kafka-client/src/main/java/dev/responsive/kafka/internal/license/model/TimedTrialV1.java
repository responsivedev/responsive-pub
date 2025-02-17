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
import java.util.Objects;

public class TimedTrialV1 extends LicenseInfo {

  public static final String TYPE_NAME = "timed_trial_v1";

  private final String email;
  private final long issuedAt;
  private final long expiresAt;

  @JsonCreator
  public TimedTrialV1(
      @JsonProperty("type") final String type,
      @JsonProperty("email") final String email,
      @JsonProperty("issuedAt") final long issuedAt,
      @JsonProperty("expiresAt") final long expiresAt
  ) {
    super(type);
    this.email = Objects.requireNonNull(email);
    this.issuedAt = issuedAt;
    this.expiresAt = expiresAt;
  }

  @JsonProperty("email")
  public String email() {
    return email;
  }

  @JsonProperty("expiresAt")
  public long expiresAt() {
    return expiresAt;
  }

  @JsonProperty("issuedAt")
  public long issuedAt() {
    return issuedAt;
  }
}
