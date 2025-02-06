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

package dev.responsive.examples.regression.tests;

import static dev.responsive.examples.regression.RegConstants.CUSTOMER_NAME_TO_LOCATION;
import static dev.responsive.examples.regression.RegConstants.CUSTOMER_ID_TO_NAME;
import static dev.responsive.examples.regression.RegConstants.ORDERS;

import dev.responsive.examples.common.InjectedE2ETestException;
import dev.responsive.examples.e2etest.Params;
import dev.responsive.examples.e2etest.UrandomGenerator;
import dev.responsive.examples.regression.RegressionSchema;
import dev.responsive.examples.regression.model.Customer;
import dev.responsive.examples.regression.model.CustomerInfo;
import dev.responsive.examples.regression.model.EnrichedOrder;
import dev.responsive.examples.regression.model.Order;
import java.time.Duration;
import java.util.Map;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;

/**
 * Slightly-more-complex example application featuring multiple types of joins (ST and FKJ) and
 * a windowed aggregation. As we add more features it would be good to expand this
 * topology further with more kinds of operators
 */
public class FullTopologyExample extends AbstractKSExampleService {

  private final UrandomGenerator randomGenerator = new UrandomGenerator();

  public FullTopologyExample(final Map<String, Object> props, final boolean responsive) {
    super(
        "stream-table-join",
        props,
        responsive
    );
  }

  // TODO(agavra): this is a simple topology to use for testing the regression framework
  // we should work on making it more robust and cover more DSL operations (perhaps as
  // individual tests)
  @Override
  public Topology buildTopology() {
    final StreamsBuilder builder = new StreamsBuilder();

    // Read orders keyed by customer id
    final KStream<String, Order> orders =
        builder.stream(ORDERS, Consumed.with(Serdes.String(), RegressionSchema.orderSerde()));

    // Read customer names keyed by customer id
    final KTable<String, String> customerIdToName =
        builder.table(CUSTOMER_ID_TO_NAME, Consumed.with(Serdes.String(), Serdes.String()));

    // Read customer location keyed by customer name
    final KTable<String, String> customerNameToLocation =
        builder.table(CUSTOMER_NAME_TO_LOCATION, Consumed.with(Serdes.String(), Serdes.String()));

    // Join customer tables to get full Customer metadata keyed by customer id
    final KTable<String, Customer> customers = customerIdToName
        .join(customerNameToLocation,
              id -> id, // join key is customer name --> extract value from customerIdToName
              (location, name) -> new CustomerInfo(name, location),
              Materialized.with(Serdes.String(), RegressionSchema.customerInfoSerde()))
        .mapValues((k, v) -> new Customer(k, v.customerName(), v.location()));


    // Enrich orders with customer information by joining the orders
    // stream with the customers table, keyed by customer id
    KStream<String, EnrichedOrder> enrichedOrders = orders
        .join(
            customers,
            EnrichedOrder::new,
            Joined.with(
                Serdes.String(),
                RegressionSchema.orderSerde(),
                RegressionSchema.customerSerde()
            )
        );

    // output to results topic
    enrichedOrders
        .peek((k, v) -> {
          if (responsive) {
            final var random = Math.abs(randomGenerator.nextLong() % 10000);
            if (random < Params.EXCEPTION_INJECT_THRESHOLD) {
              throw new InjectedE2ETestException();
            }
          }
        })
        .groupByKey()
        .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofHours(1), Duration.ofHours(12)))
        .reduce(EnrichedOrder::combineWith,
                Materialized.with(Serdes.String(), RegressionSchema.enrichedOrderSerde()))
        .toStream()
        .selectKey((w, v) -> w.key())
        .to(
            resultsTopic(),
            Produced.with(Serdes.String(), RegressionSchema.enrichedOrderSerde())
        );

    return builder.build();
  }

}
