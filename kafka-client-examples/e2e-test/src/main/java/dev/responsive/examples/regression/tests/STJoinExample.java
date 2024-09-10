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

package dev.responsive.examples.regression.tests;

import static dev.responsive.examples.regression.RegConstants.CUSTOMERS;
import static dev.responsive.examples.regression.RegConstants.ORDERS;

import dev.responsive.examples.regression.RegressionSchema;
import dev.responsive.examples.regression.model.Customer;
import dev.responsive.examples.regression.model.EnrichedOrder;
import dev.responsive.examples.regression.model.Order;
import java.util.Map;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

public class STJoinExample extends AbstractKSExampleService{

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
    enrichedOrders.to(
        resultsTopic(),
        Produced.with(Serdes.String(), RegressionSchema.enrichedOrderSerde())
    );

    return builder.build();
  }

}
