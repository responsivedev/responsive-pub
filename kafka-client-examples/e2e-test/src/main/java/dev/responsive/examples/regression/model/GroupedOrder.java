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

package dev.responsive.examples.regression.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Comparator;
import java.util.List;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;

/**
 * A batch of one or more purchases grouped into a single order
 */
public record GroupedOrder(
    @JsonProperty("orders") List<Order> orders
) implements Comparable<GroupedOrder> {

  @Override
  public int compareTo(final GroupedOrder o) {
    return Comparator
        .comparingInt((ToIntFunction<GroupedOrder>) r -> r.orders.size())
        .thenComparing(r -> r.orders.stream().map(Order::orderId).collect(Collectors.joining()))
        .compare(this, o);
  }
}
