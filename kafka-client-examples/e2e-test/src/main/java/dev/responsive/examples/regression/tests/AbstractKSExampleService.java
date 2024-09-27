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
import static dev.responsive.examples.regression.RegConstants.NUM_PARTITIONS;
import static dev.responsive.examples.regression.RegConstants.ORDERS;

import com.google.common.util.concurrent.AbstractIdleService;
import dev.responsive.examples.common.E2ETestUtils;
import dev.responsive.examples.common.UncaughtStreamsAntithesisHandler;
import dev.responsive.examples.regression.RegConstants;
import dev.responsive.kafka.api.ResponsiveKafkaStreams;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractKSExampleService extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractKSExampleService.class);

  private final Executor executor = Executors.newSingleThreadExecutor();

  protected final boolean responsive;
  private final Map<String, Object> properties;
  private final String name;

  private KafkaStreams kafkaStreams;

  public AbstractKSExampleService(
      final String name,
      final Map<String, Object> props,
      final boolean responsive
  ) {
    this.name = name;
    this.responsive = responsive;
    this.properties = E2ETestUtils.defaultStreamProps(props);
    this.properties.put(
        StreamsConfig.APPLICATION_ID_CONFIG,
        name + "-" + responsive
    );
  }

  @Override
  protected final Executor executor() {
    // override the default executor to ensure that startUp() and shutDown() run on
    // the same thread (i.e. ensuring that the service completes startUp before calling shutDown)
    return executor;
  }

  @Override
  protected final void startUp() throws Exception {
    LOG.info("Starting {}...", name);
    E2ETestUtils.maybeCreateTopics(
        properties,
        NUM_PARTITIONS,
        List.of(
            ORDERS,
            CUSTOMERS,
            resultsTopic()
        )
    );
    LOG.info("Created topics...");
    E2ETestUtils.maybeCreateKeyspace(properties);
    LOG.info("Awaiting keyspace...");
    E2ETestUtils.awaitKeyspace(properties);
    LOG.info("Created keyspace...");

    LOG.info("Starting Kafka Streams...");

    kafkaStreams = responsive
        ? new ResponsiveKafkaStreams(buildTopology(), properties)
        : new KafkaStreams(buildTopology(), new StreamsConfig(properties));
    kafkaStreams.setUncaughtExceptionHandler(new UncaughtStreamsAntithesisHandler());
    kafkaStreams.start();
    LOG.info("Kafka Streams started!");
  }

  @Override
  protected final void shutDown() throws Exception {
    if (kafkaStreams != null) {
      kafkaStreams.close();
    }
  }

  protected abstract Topology buildTopology();

  protected final String resultsTopic() {
    return RegConstants.resultsTopic(responsive);
  }

  public String name() {
    return name;
  }
}
