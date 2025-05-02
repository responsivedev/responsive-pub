/*
 * Copyright 2025 Responsive Computing, Inc.
 *
 * This source code is licensed under the Responsive Business Source License Agreement v1.0
 * available at:
 *
 * https://www.responsive.dev/legal/responsive-bsl-10
 *
 * This software requires a valid Commercial License Key for production use. Trial and commercial
 * licenses can be obtained at https://www.responsive.dev
 */

package dev.responsive.examples.rs3.demo;

import static dev.responsive.examples.regression.RegConstants.ORDERS;

import dev.responsive.examples.common.JsonSerde;
import dev.responsive.examples.regression.RegressionSchema;
import dev.responsive.examples.regression.model.CustomerOrderTracker;
import dev.responsive.examples.regression.model.Order;
import dev.responsive.examples.regression.tests.AbstractKSExampleService;
import dev.responsive.kafka.api.stores.ResponsiveStores;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;

public class OrderLimitValidator extends AbstractKSExampleService {

  public static final String CUSTOMER_ORDERS_STORE = "customer-orders";
  public static final String NAME = "order-limit-validator";
  private static final double SPEND_LIMIT = 100;
  private static final String VALIDATED_ORDERS_TOPIC = "validated-orders";

  public OrderLimitValidator(
      final Map<String, Object> props,
      final boolean responsive
  ) {
    super(NAME, props, responsive);
  }

  @Override
  protected Topology buildTopology() {
    final StreamsBuilder builder = new StreamsBuilder();
    final StoreBuilder<KeyValueStore<String, CustomerOrderTracker>> storeBuilder =
        ResponsiveStores.keyValueStoreBuilder(
            ResponsiveStores.keyValueStore(CUSTOMER_ORDERS_STORE),
            Serdes.String(),
            new JsonSerde<>(CustomerOrderTracker.class)
        );

    // Read orders from the orders topic
    final KStream<String, Order> orders =
        builder.stream(ORDERS, Consumed.with(Serdes.String(), RegressionSchema.orderSerde()));

    // repartition orders by the customer id instead of the
    // order id, so we can validate customer orders
    orders.selectKey((id, order) -> order.customerId())
        .process(new ProcessorSupplier<String, Order, String, Order>() {

      @Override
      public Set<StoreBuilder<?>> stores() {
        return Set.of(storeBuilder);
      }

      @Override
      public Processor<String, Order, String, Order> get() {
        return new Processor<>() {

          private ProcessorContext<String, Order>
              context;
          private KeyValueStore<String, CustomerOrderTracker> stateStore;

          @Override
          public void init(final ProcessorContext<String, Order> context) {
            stateStore = context.getStateStore(CUSTOMER_ORDERS_STORE);
            this.context = context;
          }

          @Override
          public void process(final Record<String, Order> record) {
            final String customerId = record.key();
            final var tracker =
                stateStore.putIfAbsent(customerId, new CustomerOrderTracker(customerId, 0d));

            // the bug is that we don't validate OrderID is not a duplicate
            final double old = tracker == null ? 0 : tracker.totalSpend();
            final double total = old + record.value().amount();
            if (total < SPEND_LIMIT) {
              final var newTracker = new CustomerOrderTracker(customerId, total);
              stateStore.put(customerId, newTracker);
              context.forward(new Record<>(
                  record.value().orderId(),
                  record.value(),
                  record.timestamp()
              ));
            }
          }
        };
      }
    }).to(VALIDATED_ORDERS_TOPIC, Produced.with(Serdes.String(), new JsonSerde<>(Order.class)));

    return builder.build();
  }
}
