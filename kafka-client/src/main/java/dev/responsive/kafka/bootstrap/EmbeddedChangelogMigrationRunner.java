package dev.responsive.kafka.bootstrap;

import dev.responsive.kafka.api.ResponsiveKafkaStreams;
import dev.responsive.kafka.api.config.ResponsiveConfig;
import dev.responsive.kafka.api.config.ResponsiveMode;
import dev.responsive.kafka.api.stores.ResponsiveKeyValueBytesStoreSupplier;
import dev.responsive.kafka.internal.stores.StoreAccumulator;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.TopologyDescription.Node;
import org.apache.kafka.streams.TopologyDescription.Processor;
import org.apache.kafka.streams.TopologyDescription.Subtopology;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorContextUtils;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.processor.internals.namedtopology.NamedTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EmbeddedChangelogMigrationRunner {
  private static final Logger LOG = LoggerFactory.getLogger(EmbeddedChangelogMigrationRunner.class);

  private EmbeddedChangelogMigrationRunner() {
  }

  public static void runMigration(
      final ResponsiveConfig responsiveConfig,
      final String applicationId,
      final Topology topology
  ) {
    final var kvBytesStores = StoreAccumulator.INSTANCE.getRegisteredKeyValueBytesStoreSuppliers();
    if (kvBytesStores.size() != 1) {
      throw new IllegalStateException("migration only supported for a single state store");
    }
    final ResponsiveKeyValueBytesStoreSupplier supplier = kvBytesStores.get(0);
    final String storeName = supplier.name();
    validateStoreIsNotSource(storeName, topology);
    final Properties properties = new Properties();
    properties.putAll(responsiveConfig.originals());
    final String changelogTopic = changelogFor(
        responsiveConfig.originals(),
        applicationId,
        storeName,
        topology instanceof NamedTopology ? ((NamedTopology) topology).name() : null
    );
    final ChangelogMigrationTool tool = new ChangelogMigrationTool(
        properties,
        supplier,
        changelogTopic
    );
    properties.put(ResponsiveConfig.RESPONSIVE_MODE, ResponsiveMode.RUN.name());
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId + "-migrate");
    final ResponsiveKafkaStreams app = tool.buildStreams();
    final CountDownLatch closed = new CountDownLatch(1);
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      app.close(Duration.ofMinutes(5));
      closed.countDown();
    }));
    LOG.info("starting bootstrap application for store {}", storeName);
    app.start();
    try {
      LOG.info("blocking until JVM is shut down");
      closed.await();
    } catch (final InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private static void validateStoreIsNotSource(final String storeName, final Topology topology) {
    final TopologyDescription desc = topology.describe();
    for (final Subtopology st : desc.subtopologies()) {
      for (final Node n : st.nodes()) {
        if (n instanceof Processor) {
          final Processor processor = (Processor) n;
          if (processor.stores().contains(storeName)) {
            LOG.info("found store with name {} in processor {}", storeName, processor.name());
            return;
          }
        }
      }
    }
    throw new RuntimeException("Could not find processor with store. " +
        "Source and global stores not supported with embedded migration");
  }

  private static String changelogFor(
      final Map<String, Object> configs,
      final String applicationId,
      final String storeName,
      final String topologyName
  ) {
    final String prefix = ProcessorContextUtils.getPrefix(configs, applicationId);
    return ProcessorStateManager.storeChangelogTopic(prefix, storeName, topologyName);
  }
}
