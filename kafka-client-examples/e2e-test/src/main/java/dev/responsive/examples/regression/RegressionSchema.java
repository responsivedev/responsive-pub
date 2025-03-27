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

package dev.responsive.examples.regression;

import dev.responsive.examples.common.JsonDeserializer;
import dev.responsive.examples.common.JsonSerde;
import dev.responsive.examples.common.JsonSerializer;
import dev.responsive.examples.regression.model.Customer;
import dev.responsive.examples.regression.model.CustomerInfo;
import dev.responsive.examples.regression.model.EnrichedOrder;
import dev.responsive.examples.regression.model.GroupedOrder;
import dev.responsive.examples.regression.model.Order;
import dev.responsive.examples.regression.model.OrderMetadata;
import org.apache.kafka.common.serialization.Serde;

public class RegressionSchema {

  public static Serde<Order> orderSerde() {
    return new JsonSerde<>(Order.class);
  }

  public static Serde<Customer> customerSerde() {
    return new JsonSerde<>(Customer.class);
  }

  public static Serde<CustomerInfo> customerInfoSerde() {
    return new JsonSerde<>(CustomerInfo.class);
  }

  public static Serde<EnrichedOrder> enrichedOrderSerde() {
    return new JsonSerde<>(EnrichedOrder.class);
  }

  public static Serde<GroupedOrder> groupedOrderSerde() {
    return new JsonSerde<>(GroupedOrder.class);
  }

  public static Serde<OrderMetadata> orderMetadataSerde() {
    return new JsonSerde<>(OrderMetadata.class);
  }

  public static class CustomerSerializer extends JsonSerializer<Customer> {
    public CustomerSerializer() {
      super(Customer.class);
    }
  }

  public static class CustomerInfoSerializer extends JsonSerializer<CustomerInfo> {
    public CustomerInfoSerializer() {
      super(CustomerInfo.class);
    }
  }

  public static class CustomerInfoDeserializer extends JsonDeserializer<CustomerInfo> {
    public CustomerInfoDeserializer() {
      super(CustomerInfo.class);
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
