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
