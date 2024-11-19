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
import java.util.Objects;

public class TimedTrialV1 extends LicenseInfo {
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
