/*
 * Copyright 2024 Responsive Computing, Inc.
 *
 * This source code is licensed under the Responsive Software License Agreement v1.0 available at
 *
 * https://www.responsive.dev/legal/responsive-bsl-10
 *
 * This software requires a valid Commercial License Key for production use. Trial and commercial
 * licenses can be obtained at https://www.responsive.dev/sdk/get-started.
 */

package dev.responsive.kafka.internal.license.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Base64;

@JsonIgnoreProperties(ignoreUnknown = true)
public class LicenseDocumentV1 extends LicenseDocument {
  private final String info;
  private final String signature;
  private final String key;
  private final String algo;

  @JsonCreator
  public LicenseDocumentV1(
      @JsonProperty("version") final String version,
      @JsonProperty("info") final String info,
      @JsonProperty("signature") final String signature,
      @JsonProperty("key") final String key,
      @JsonProperty("algo") final String algo
  ) {
    super(version);
    this.info = info;
    this.signature = signature;
    this.key = key;
    this.algo = algo;
  }

  public String info() {
    return info;
  }

  public String key() {
    return key;
  }

  public String signature() {
    return signature;
  }

  public String algo() {
    return algo;
  }

  public byte[] decodeInfo() {
    return Base64.getDecoder().decode(info);
  }

  public byte[] decodeSignature() {
    return Base64.getDecoder().decode(signature);
  }
}
