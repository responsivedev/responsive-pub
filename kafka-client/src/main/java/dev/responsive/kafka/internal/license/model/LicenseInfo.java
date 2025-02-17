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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.EXISTING_PROPERTY,
    property = "type",
    visible = true
)
@JsonSubTypes({
    @JsonSubTypes.Type(value = TimedTrialV1.class, name = TimedTrialV1.TYPE_NAME),
    @JsonSubTypes.Type(value = CloudLicenseV1.class, name = CloudLicenseV1.TYPE_NAME),
    @JsonSubTypes.Type(value = UsageBasedV1.class, name = UsageBasedV1.TYPE_NAME)
})
public abstract class LicenseInfo {
  private final String type;

  LicenseInfo(@JsonProperty("type") final String type) {
    this.type = type;
  }

  @JsonProperty("type")
  public String type() {
    return type;
  }
}
