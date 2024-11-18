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

import static dev.responsive.examples.regression.RegConstants.CUSTOMERS;
import static dev.responsive.examples.regression.RegConstants.ORDERS;

import dev.responsive.examples.common.InjectedE2ETestException;
import dev.responsive.examples.e2etest.Params;
import dev.responsive.examples.e2etest.UrandomGenerator;
import dev.responsive.examples.regression.RegressionSchema;
import dev.responsive.examples.regression.model.Customer;
import dev.responsive.examples.regression.model.EnrichedOrder;
import dev.responsive.examples.regression.model.Order;
import java.time.Duration;
import java.util.Map;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;

public class STJoinExample extends AbstractKSExampleService {

  private final UrandomGenerator randomGenerator = new UrandomGenerator();

  public STJoinExample(final Map<String, Object> props, final boolean responsive) {
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
  protected Topology buildTopology() {
    final StreamsBuilder builder = new StreamsBuilder();

    // Read orders from the orders topic
    final KStream<String, Order> orders =
        builder.stream(ORDERS, Consumed.with(Serdes.String(), RegressionSchema.orderSerde()));


    // Read customers from the customers topic
    final KTable<String, Customer> customers =
        builder.table(CUSTOMERS, Consumed.with(Serdes.String(), RegressionSchema.customerSerde()));

    // Enrich orders with customer information by joining the orders
    // stream with the customers table
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
        .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofDays(1), Duration.ofHours(12)))
        .reduce(EnrichedOrder::combineWith)
        .toStream()
        .selectKey((w, v) -> w.key())
        .to(
            resultsTopic(),
            Produced.with(Serdes.String(), RegressionSchema.enrichedOrderSerde())
        );

    return builder.build();
  }

}
