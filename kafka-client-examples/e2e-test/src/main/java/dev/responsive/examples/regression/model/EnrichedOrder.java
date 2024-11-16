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
import java.util.Comparator;
import java.util.function.Function;

public record EnrichedOrder(
    @JsonProperty("order") Order order,
    @JsonProperty("customer") Customer customer
) implements Comparable<EnrichedOrder> {

  @Override
  public int compareTo(final EnrichedOrder o) {
    return Comparator
        // the order id is of form "order_{timestamp}_{random_int}" -- extract
        // the timestamp and use that as the comparison key
        .comparing((Function<EnrichedOrder, Long>) r -> Long.valueOf(order.orderId().split("_")[1]))
        // we just need to compare the key / amount / customer name to verify
        // that things worked
        .thenComparing(r -> order.amount())
        .thenComparing(r -> customer.customerName())
        .compare(this, o);
  }
}