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

package dev.responsive.examples.regression;

import dev.responsive.examples.common.JsonDeserializer;
import dev.responsive.examples.common.JsonSerde;
import dev.responsive.examples.common.JsonSerializer;
import dev.responsive.examples.regression.model.Customer;
import dev.responsive.examples.regression.model.EnrichedOrder;
import dev.responsive.examples.regression.model.GroupedOrder;
import dev.responsive.examples.regression.model.Order;
import dev.responsive.examples.regression.model.StoredOrder;
import org.apache.kafka.common.serialization.Serde;

public class RegressionSchema {

  public static Serde<Order> orderSerde() {
    return new JsonSerde<>(Order.class);
  }

  public static Serde<Customer> customerSerde() {
    return new JsonSerde<>(Customer.class);
  }

  public static Serde<EnrichedOrder> enrichedOrderSerde() {
    return new JsonSerde<>(EnrichedOrder.class);
  }

  public static Serde<GroupedOrder> groupedOrderSerde() {
    return new JsonSerde<>(GroupedOrder.class);
  }

  public static Serde<StoredOrder> storedOrderSerde() {
    return new JsonSerde<>(StoredOrder.class);
  }

  public static class CustomerSerializer extends JsonSerializer<Customer> {
    public CustomerSerializer() {
      super(Customer.class);
    }
  }

  public static class OrderSerializer extends JsonSerializer<Order> {
    public OrderSerializer() {
      super(Order.class);
    }
  }

  public static class EnrichedOrderDeserializer extends JsonDeserializer<EnrichedOrder> {
    public EnrichedOrderDeserializer() {
      super(EnrichedOrder.class);
    }
  }

  public static class GroupedOrderDeserializer extends JsonDeserializer<GroupedOrder> {
    public GroupedOrderDeserializer() {
      super(GroupedOrder.class);
    }
  }

}
