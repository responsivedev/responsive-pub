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

package dev.responsive.kafka.api;

import static dev.responsive.kafka.internal.stores.TTDRestoreListener.mockRestoreListener;

import dev.responsive.kafka.api.config.ResponsiveConfig;
import dev.responsive.kafka.internal.clients.TTDCassandraClient;
import dev.responsive.kafka.internal.clients.TTDMockAdmin;
import dev.responsive.kafka.internal.config.InternalSessionConfigs;
import dev.responsive.kafka.internal.utils.SessionClients;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.Properties;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.TopologyTestDriver;

public class ResponsiveTopologyTestDriver extends TopologyTestDriver {
  public static final String RESPONSIVE_TTD_ORG = "Responsive";
  public static final String RESPONSIVE_TTD_ENV = "TopologyTestDriver";

  private final TTDCassandraClient client;

  /**
   * Create a new test diver instance with default test properties.
   * <p>
   * Can be used exactly the same as the usual {@link TopologyTestDriver}
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
   * <p>
   * Can be used exactly the same as the usual {@link TopologyTestDriver}
   *
   * @param topology the topology to be tested
   * @param config   the configuration for the topology
   */
  public ResponsiveTopologyTestDriver(final Topology topology, final Properties config) {
    this(topology, config, null);
  }

  /**
   * Create a new test diver instance with default test properties.
   * <p>
   * Can be used exactly the same as the usual {@link TopologyTestDriver}
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
   * <p>
   * Can be used exactly the same as the usual {@link TopologyTestDriver}
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

    final SessionClients sessionClients = new SessionClients(
        Optional.empty(), Optional.of(client), false, client.mockAdmin()
    );
    final var restoreListener = mockRestoreListener(props);
    sessionClients.initialize(restoreListener.metrics(), restoreListener);

    props.putAll(new InternalSessionConfigs.Builder()
        .withSessionClients(sessionClients)
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

    props.put(ResponsiveConfig.RESPONSIVE_ORG_CONFIG, RESPONSIVE_TTD_ORG);
    props.put(ResponsiveConfig.RESPONSIVE_ENV_CONFIG, RESPONSIVE_TTD_ENV);
    props.put(ResponsiveConfig.STORE_FLUSH_INTERVAL_TRIGGER_MS_CONFIG, 0);

    props.putIfAbsent(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
    props.putIfAbsent(
        StreamsConfig.APPLICATION_ID_CONFIG,
        RESPONSIVE_TTD_ORG + "-" + RESPONSIVE_TTD_ENV
    );
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
