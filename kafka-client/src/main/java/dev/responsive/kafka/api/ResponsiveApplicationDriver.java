package dev.responsive.kafka.api;

import dev.responsive.db.CassandraClient;
import dev.responsive.kafka.clients.ResponsiveKafkaClientSupplier;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsConfig.InternalConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.internals.assignment.StickyTaskAssignor;
import org.slf4j.Logger;

public class ResponsiveApplicationDriver implements StreamsApplicationDriver {

  private final Logger logger;

  private final StreamsConfig streamsConfig;
  private final KafkaClientSupplier clientSupplier;
  private final Admin admin;
  private final ScheduledExecutorService executor = new ScheduledThreadPoolExecutor(2);

  private final StreamsStoreDriver storeDriver;

  // TODO(sophie): allow users to separately configure Responsive clients and Streams app?
  public ResponsiveApplicationDriver(
      final CassandraClient client,
      final Map<String, Object> appConfigs
  ) {
    streamsConfig = configureStreams(appConfigs);
    final String appId = streamsConfig.getString(StreamsConfig.APPLICATION_ID_CONFIG);
    logger = new LogContext(String.format("app-driver [%s] ", appId)).logger(ResponsiveDriver.class);

    clientSupplier = new ResponsiveKafkaClientSupplier();
    admin = clientSupplier.getAdmin(appConfigs);
    storeDriver = new ResponsiveStoreDriver(client, executor, admin);
  }

  // TODO(sophie): what other configs might we want to override here, or prevent
  //  users from overriding
  private StreamsConfig configureStreams(final Map<String, Object> appConfigs) {
    final Properties props = new Properties(appConfigs.size());
    props.putAll(appConfigs);

    props.put(InternalConfig.INTERNAL_TASK_ASSIGNOR_CLASS, StickyTaskAssignor.class);
    props.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 0);

    if (props.containsKey(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG)) {
      final Set<String> optimizationConfigs = StreamsConfig.verifyTopologyOptimizationConfigs(
          props.getProperty(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG)
      );
      logger.debug("Validating topology optimization configs: found {}", optimizationConfigs);

      if (optimizationConfigs.contains(StreamsConfig.REUSE_KTABLE_SOURCE_TOPICS)) {
        logger.error("Incompatible configuration: topology optimization was set to {}",
                     optimizationConfigs);
        throw new RuntimeException("Found invalid configuration: the source topic changelog "
                                       + "optimization is not supported at this time");
      } else {
        logger.debug("Validated topology optimization configs: {}", optimizationConfigs);
      }
    }

    return new StreamsConfig(props);
  }

  @Override
  public StreamsConfig streamsConfig() {
    return streamsConfig;
  }

  @Override
  public StreamsStoreDriver storeDriver() {
    return storeDriver;
  }

  @Override
  public KafkaStreams kafkaStreams(final Topology topology) {
    return new KafkaStreams(topology, streamsConfig, clientSupplier) {
      // this is kind of hacky, but it makes things easier on the user as they don't  have to
      // manage any additional resources if we make sure they all get closed alongside Streams
      @Override
      public void close() {
        super.close();
        ResponsiveApplicationDriver.this.close();
      }

      @Override
      public boolean close(final Duration timeout) {
        final boolean closed = super.close(timeout);
        ResponsiveApplicationDriver.this.close();
        return closed;
      }

      @Override
      public boolean close(final CloseOptions options) {
        final boolean closed = super.close(options);
        ResponsiveApplicationDriver.this.close();
        return closed;
      }
    };
  }

  @Override
  public void close() {
    executor.shutdown();
    admin.close();
  }
}
