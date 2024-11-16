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

package dev.responsive.examples.regression.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Optional;

public record StoredOrder(
    @JsonProperty("order") Optional<Order> order,
    @JsonProperty("meta") Optional<Meta> meta
) {

  public record Meta(
      @JsonProperty("timestamp") long timestamp,
      @JsonProperty("count") long count,
      @JsonProperty("size") long size
  ) {
  }
}
