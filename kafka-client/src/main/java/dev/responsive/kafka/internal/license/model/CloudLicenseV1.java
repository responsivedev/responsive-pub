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

public class CloudLicenseV1 extends LicenseInfo {

  public static final String TYPE_NAME = "cloud_license_v1";

  private final String key;

  @JsonCreator
  public CloudLicenseV1(
      @JsonProperty("type") final String type,
      @JsonProperty("key") final String key
  ) {
    super(type);
    this.key = key;
  }

  @JsonProperty("key")
  public String key() {
    return key;
  }
}
