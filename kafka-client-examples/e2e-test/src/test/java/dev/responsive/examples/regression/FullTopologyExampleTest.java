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

import static dev.responsive.examples.regression.RegConstants.CUSTOMER_ID_TO_NAME;
import static dev.responsive.examples.regression.RegConstants.CUSTOMER_NAME_TO_LOCATION;
import static dev.responsive.examples.regression.RegConstants.ORDERS;
import static dev.responsive.examples.regression.RegConstants.resultsTopic;
import static dev.responsive.examples.regression.RegressionSchema.enrichedOrderSerde;
import static org.apache.kafka.streams.StreamsConfig.COMMIT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG;

import dev.responsive.examples.regression.model.EnrichedOrder;
import dev.responsive.examples.regression.model.Order;
import dev.responsive.examples.regression.tests.FullTopologyExample;
import dev.responsive.kafka.api.ResponsiveTopologyTestDriver;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.junit.jupiter.api.Test;

public class FullTopologyExampleTest {

  public static final String CUSTOMER_ID_1 = "1";
  public static final String CUSTOMER_NAME_1 = "Anna";
  public static final String CUSTOMER_LOCATION_1 = "Anaheim";

  @Test
  public void shouldMatchResults() {
    final Map<String, Object> props = getBaselineProps();
    final Topology responsiveTopology = new FullTopologyExample(props, true).buildTopology();

    try (final var driver = new ResponsiveTopologyTestDriver(responsiveTopology)) {
      final TestInputTopic<String, String> customerIdToName = driver.createInputTopic(
          CUSTOMER_ID_TO_NAME, new StringSerializer(), new StringSerializer());
      final TestInputTopic<String, String> customerNameToLocation = driver.createInputTopic(
          CUSTOMER_NAME_TO_LOCATION, new StringSerializer(), new StringSerializer());
      final TestInputTopic<String, Order> orders = driver.createInputTopic(
          ORDERS, new StringSerializer(), RegressionSchema.orderSerde().serializer());

      final TestOutputTopic<String, EnrichedOrder> output = driver.createOutputTopic(
          resultsTopic(true), new StringDeserializer(), enrichedOrderSerde().deserializer());


      customerIdToName.pipeInput(CUSTOMER_ID_1, CUSTOMER_NAME_1, 0);
      customerNameToLocation.pipeInput(CUSTOMER_NAME_1, CUSTOMER_LOCATION_1, 0);

      orders.pipeInput(CUSTOMER_ID_1, new Order("order_1", CUSTOMER_ID_1, 5d), 0);

      driver.flush();
      final var read = output.readValuesToList();

      System.out.println("SOPHIE: final result --> " + read);
    }

  }

  private Map<String, Object> getBaselineProps() {
    final Map<String, Object> properties = new HashMap<>();

    properties.put(COMMIT_INTERVAL_MS_CONFIG, 0);
    properties.put(STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
    return properties;
  }
}
