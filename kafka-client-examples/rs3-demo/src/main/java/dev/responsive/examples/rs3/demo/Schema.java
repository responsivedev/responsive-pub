package dev.responsive.examples.rs3.demo;

import dev.responsive.examples.common.JsonDeserializer;
import dev.responsive.examples.common.JsonSerde;
import dev.responsive.examples.common.JsonSerializer;
import org.apache.kafka.common.serialization.Serde;

public class Schema {

  public static Serde<Order> orderSerde() {
    return new JsonSerde<>(Order.class);
  }

  public static class OrderSerializer extends JsonSerializer<Order> {
    public OrderSerializer() {
      super(Order.class);
    }
  }

  public static class OrderDeserializer extends JsonDeserializer<Order> {
    public OrderDeserializer() {
      super(Order.class);
    }
  }
}