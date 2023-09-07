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

import dev.responsive.kafka.clients.TTDCassandraClient;
import dev.responsive.kafka.clients.TTDMockAdmin;
import dev.responsive.kafka.config.ResponsiveConfig;
import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.TopologyTestDriver;

public class ResponsiveTopologyTestDriver extends TopologyTestDriver {
  public static final String RESPONSIVE_TTD_ID = "Responsive_TopologyTestDriver";

  private final TTDCassandraClient client;

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
    this(
        topology,
        config,
        initialWallClockTime,
        new TTDCassandraClient(
            new TTDMockAdmin(baseProps(config), topology),
            mockTime(initialWallClockTime))
    );
  }

  /**
   * Advances the internal mock time used for Responsive-specific features such as ttl, as well
   * as the mock time used for various Kafka Streams functionality such as wall-clock punctuators.
   * See {@link TopologyTestDriver#advanceWallClockTime(Duration)} for more details.
   *
   * @param advance the amount of time to advance wall-clock time
   */
  @Override
  public void advanceWallClockTime(final Duration advance) {
    client.advanceWallClockTime(advance);
    super.advanceWallClockTime(advance);
  }

  private ResponsiveTopologyTestDriver(
      final Topology topology,
      final Properties config,
      final Instant initialWallClockTime,
      final TTDCassandraClient cassandraClient
  ) {
    super(
        topology,
        testDriverProps(config, topology.describe(), cassandraClient),
        initialWallClockTime
    );
    this.client = cassandraClient;
  }

  private static Properties testDriverProps(
      final Properties userProps,
      final TopologyDescription topologyDescription,
      final TTDCassandraClient client
  ) {
    final Properties props = baseProps(userProps);

    props.putAll(new InternalConfigs.Builder()
        .withCassandraClient(client)
        .withKafkaAdmin(client.mockAdmin())
        .withExecutorService(new ScheduledThreadPoolExecutor(1))
        .withStoreRegistry(client.storeRegistry())
        .withTopologyDescription(topologyDescription)
        .build()
    );
    return props;
  }

  @SuppressWarnings("deprecation")
  private static Properties baseProps(final Properties userProps) {
    final Properties props = new Properties();
    props.putAll(userProps);

    props.put(ResponsiveConfig.TENANT_ID_CONFIG, RESPONSIVE_TTD_ID);
    props.put(ResponsiveConfig.STORE_FLUSH_INTERVAL_TRIGGER_MS_CONFIG, 0);

    props.putIfAbsent(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
    props.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, RESPONSIVE_TTD_ID);
    return props;
  }

  private static MockTime mockTime(final Instant initialWallClockTime) {
    final MockTime mockTime = new MockTime(0L, 0L, 0L);
    final long initialWallClockTimeMs = initialWallClockTime == null
        ? System.currentTimeMillis()
        : initialWallClockTime.toEpochMilli();

    mockTime.setCurrentTimeMs(initialWallClockTimeMs);
    return mockTime;
  }

}
