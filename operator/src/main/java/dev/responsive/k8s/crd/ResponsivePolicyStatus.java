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

package dev.responsive.k8s.crd;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ResponsivePolicyStatus {
  private final String message;

  public ResponsivePolicyStatus(@JsonProperty("message") final String message) {
    this.message = message;
  }

  public String getMessage() {
    return message;
  }
}
