package dev.responsive.kafka.internal.snapshot;

import dev.responsive.kafka.api.config.ResponsiveConfig;
import dev.responsive.kafka.internal.snapshot.topic.TopicSnapshotStore;
import dev.responsive.kafka.internal.utils.TopologyTaskInfo;
import java.util.Optional;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyDescription;

public class KafkaStreamsSnapshotContext {
  private final SnapshotOrchestrator orchestrator;
  private final GenerationStorage generationStorage;
  private final TopologyTaskInfo topologyTaskInfo;
  private final SnapshotStore snapshotStore;

  private KafkaStreamsSnapshotContext(
      final SnapshotOrchestrator orchestrator,
      final GenerationStorage generationStorage,
      final SnapshotStore snapshotStore,
      final TopologyTaskInfo topologyTaskInfo
  ) {
    this.orchestrator = orchestrator;
    this.generationStorage = generationStorage;
    this.snapshotStore = snapshotStore;
    this.topologyTaskInfo = topologyTaskInfo;
  }

  public SnapshotOrchestrator orchestrator() {
    return orchestrator;
  }

  public GenerationStorage generationStorage() {
    return generationStorage;
  }

  public TopologyTaskInfo topologyTaskInfo() {
    return topologyTaskInfo;
  }

  public static Optional<KafkaStreamsSnapshotContext> create(
      final ResponsiveConfig config,
      final StreamsConfig streamsConfig,
      final TopologyDescription topologyDescription
  ) {
    final SnapshotSupport support = SnapshotSupport
        .valueOf(config.getString(ResponsiveConfig.SNAPSHOTS_CONFIG));
    switch (support) {
      case LOCAL: {
        final SnapshotStore store = TopicSnapshotStore.create(config.originals());
        final TopologyTaskInfo tti;
        try (final Admin admin = Admin.create(config.originals())) {
          tti = TopologyTaskInfo.forTopology(
              topologyDescription,
              admin
          );
        }
        final SnapshotOrchestrator orchestrator = new LocalSnapshotOrchestrator(
            store,
            tti.partitionsByTask().keySet()
        );
        final GenerationStorage generationStorage = new SnapshotStoreBasedGenerationStorage(store);
        return Optional.of(new KafkaStreamsSnapshotContext(
            orchestrator,
            generationStorage,
            store,
            tti
        ));
      }
      default: {
        return Optional.empty();
      }
    }
  }
}
