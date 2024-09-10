/*
 * Copyright 2023 Responsive Computing, Inc.
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