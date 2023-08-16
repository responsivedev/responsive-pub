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

package dev.responsive.kafka.api;

import dev.responsive.kafka.config.ResponsiveConfig;
import dev.responsive.kafka.store.ResponsiveStoreRegistry;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.MockAdminClient;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.TopologyTestDriver;

public class ResponsiveTopologyTestDriver extends TopologyTestDriver {

  /**
   * Create a new test diver instance.
   * Default test properties are used to initialize the driver instance
   *
   * @param topology the topology to be tested
   */
  public ResponsiveTopologyTestDriver(final Topology topology) {
    this(topology, new Properties());
  }

  /**
   * Create a new test diver instance.
   * Initialized the internally mocked wall-clock time with
   * {@link System#currentTimeMillis() current system time}.
   *
   * @param topology the topology to be tested
   * @param config   the configuration for the topology
   */
  public ResponsiveTopologyTestDriver(final Topology topology, final Properties config) {
    this(topology, config, null);
  }

  /**
   * Create a new test diver instance.
   *
   * @param topology the topology to be tested
   * @param initialWallClockTimeMs the initial value of internally mocked wall-clock time
   */
  public ResponsiveTopologyTestDriver(
      final Topology topology,
      final Instant initialWallClockTimeMs
  ) {
    this(topology, new Properties(), initialWallClockTimeMs);
  }

  /**
   * Create a new test diver instance.
   *
   * @param topology               the topology to be tested
   * @param config                 the configuration for the topology
   * @param initialWallClockTime   the initial value of internally mocked wall-clock time
   */
  public ResponsiveTopologyTestDriver(
      final Topology topology,
      final Properties config,
      final Instant initialWallClockTime
  ) {
    super(topology, testDriverProps(config, topology.describe()), initialWallClockTime);
  }

  private static Properties testDriverProps(
      final Properties baseProps,
      final TopologyDescription topologyDescription
  ) {
    final Properties props = new Properties();
    props.putAll(baseProps);
    props.put(ResponsiveConfig.TENANT_ID_CONFIG, "topology-test-driver");

    props.putAll(new InternalConfigs.Builder()
        .withCassandraClient(new CassandraClientStub(props))
        .withKafkaAdmin(new TTDMockAdmin())
        .withExecutorService(new ScheduledThreadPoolExecutor(1))
        .withStoreRegistry(new ResponsiveStoreRegistry())
        .withTopologyDescription(topologyDescription)
        .build()
    );
    return props;
  }

  private static class TTDMockAdmin extends MockAdminClient {
    private static final Node BROKER = new Node(0, "dummyHost-1", 1234);
    public TTDMockAdmin() {
      super(Collections.singletonList(BROKER), BROKER);
    }

    @Override
    public DescribeTopicsResult describeTopics(Collection<String> topicNames) {
      for (final String topic : topicNames) {
        addTopic(
            true,
            topic,
            Collections.singletonList(
                new TopicPartitionInfo(0, BROKER, Collections.emptyList(), Collections.emptyList())),
            Collections.emptyMap()
        );
      }
      return super.describeTopics(topicNames);
    }
  }


}
