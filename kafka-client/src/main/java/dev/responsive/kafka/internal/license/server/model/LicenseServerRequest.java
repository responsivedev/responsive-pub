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

package dev.responsive.kafka.internal.license.server.model;

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
    @JsonSubTypes.Type(value = OriginEventsReportRequestV1.class, name = OriginEventsReportRequestV1.TYPE_NAME)
})
public class LicenseServerRequest {

  private final String type;
  private final long timestamp;

  public LicenseServerRequest(
      @JsonProperty("type") final String type,
      @JsonProperty("timestamp") final long timestamp
  ) {
    this.type = type;
    this.timestamp = timestamp;
  }

  @JsonProperty("type")
  public String type() {
    return type;
  }

  @JsonProperty("timestamp")
  public long timestamp() {
    return timestamp;
  }
}
