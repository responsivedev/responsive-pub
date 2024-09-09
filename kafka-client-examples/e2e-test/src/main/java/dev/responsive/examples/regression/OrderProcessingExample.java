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

import static dev.responsive.examples.regression.RegConstants.CUSTOMERS;
import static dev.responsive.examples.regression.RegConstants.NUM_PARTITIONS;
import static dev.responsive.examples.regression.RegConstants.ORDERS;
import static dev.responsive.examples.regression.RegConstants.resultsTopic;

import com.google.common.util.concurrent.AbstractIdleService;
import dev.responsive.examples.common.E2ETestUtils;
import dev.responsive.examples.common.UncaughtStreamsAntithesisHandler;
import dev.responsive.examples.regression.model.Customer;
import dev.responsive.examples.regression.model.EnrichedOrder;
import dev.responsive.examples.regression.model.Order;
import dev.responsive.kafka.api.ResponsiveKafkaStreams;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.BuiltInDslStoreSuppliers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OrderProcessingExample extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(OrderProcessingExample.class);

  private final Executor executor = Executors.newSingleThreadExecutor();
  private final Map<String, Object> properties;
  private final boolean responsive;
  private KafkaStreams kafkaStreams;

  public OrderProcessingExample(final Map<String, Object> props, final boolean responsive) {
    this.responsive = responsive;
    this.properties = new HashMap<>(props);

    this.properties.put(
        StreamsConfig.APPLICATION_ID_CONFIG,
        "order-processing-example-" + responsive
    );
    this.properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
    this.properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
    this.properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
    this.properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);

    this.properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);
    this.properties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 60000);

    this.properties.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 90000);

    if (!responsive) {
      this.properties.put(
          StreamsConfig.DSL_STORE_SUPPLIERS_CLASS_CONFIG,
          BuiltInDslStoreSuppliers.InMemoryDslStoreSuppliers.class
      );
    }
  }

  @Override
  protected void startUp() {
    LOG.info("Starting OrderProcessingExample...");
    E2ETestUtils.maybeCreateTopics(
        properties,
        NUM_PARTITIONS,
        List.of(ORDERS, CUSTOMERS, resultsTopic(true), resultsTopic(false))
    );
    LOG.info("Created topics...");
    E2ETestUtils.maybeCreateKeyspace(properties);
    LOG.info("Created keyspace...");
    E2ETestUtils.awaitKeyspace(properties);

    LOG.info("Starting Kafka Streams...");
    kafkaStreams = responsive
        ? new ResponsiveKafkaStreams(buildTopology(), properties)
        : new KafkaStreams(buildTopology(), new StreamsConfig(properties));
    kafkaStreams.setUncaughtExceptionHandler(new UncaughtStreamsAntithesisHandler());
    kafkaStreams.start();
    LOG.info("Kafka Streams started!");
  }

  @Override
  protected void shutDown() {
    if (kafkaStreams != null) {
      kafkaStreams.close();
    }
  }

  @Override
  protected Executor executor() {
    // override the default executor to ensure that startUp() and shutDown() run on
    // the same thread (i.e. ensuring that the service completes startUp before calling shutDown)
    return executor;
  }

  // TODO(agavra): this is a simple topology to use for testing the regression framework
  // we should work on making it more robust and cover more DSL operations (perhaps as
  // individual tests)
  private Topology buildTopology() {
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
        resultsTopic(responsive),
        Produced.with(Serdes.String(), RegressionSchema.enrichedOrderSerde())
    );

    return builder.build();
  }

}
