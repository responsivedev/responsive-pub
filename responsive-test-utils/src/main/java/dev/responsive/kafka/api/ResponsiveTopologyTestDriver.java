package dev.responsive.kafka.api;

import static dev.responsive.kafka.clients.SharedClients.sharedClientConfigs;

import dev.responsive.db.CassandraClient;
import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import org.apache.kafka.clients.admin.MockAdminClient;
import org.apache.kafka.streams.Topology;
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
    super(topology, testDriverProps(config), initialWallClockTime);
  }

  private static Properties testDriverProps(final Properties baseProps) {
    final Properties props = new Properties();
    props.putAll(baseProps);
    props.putAll(sharedClientConfigs(
        new CassandraClientStub(),
        new MockAdminClient(),
        new ScheduledThreadPoolExecutor(1))
    );
    return props;
  }

  private static class CassandraClientStub extends CassandraClient {
    CassandraClientStub() {
      super();
    }

    // TODO: override CassandraClient methods to stash data in a local in-memory stub
  }

}
