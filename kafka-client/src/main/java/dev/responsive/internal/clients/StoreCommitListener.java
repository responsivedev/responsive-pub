package dev.responsive.internal.clients;

import dev.responsive.internal.clients.OffsetRecorder.RecordingKey;
import dev.responsive.internal.stores.ResponsiveStoreRegistration;
import dev.responsive.internal.stores.ResponsiveStoreRegistry;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.common.TopicPartition;

public class StoreCommitListener {
  private final ResponsiveStoreRegistry storeRegistry;

  public StoreCommitListener(
      final ResponsiveStoreRegistry storeRegistry,
      final OffsetRecorder offsetRecorder
  ) {
    this.storeRegistry = Objects.requireNonNull(storeRegistry);
    offsetRecorder.addCommitCallback(this::onCommit);
  }

  private void onCommit(
      final Map<RecordingKey, Long> committedOffsets,
      final Map<TopicPartition, Long> writtenOffsets
  ) {
    // TODO: this is kind of inefficient (iterates over stores a lot). Not a huge deal
    //       as we only run this on each commit. Can fix by indexing stores in registry
    for (final var e : committedOffsets.entrySet()) {
      final TopicPartition p = e.getKey().getPartition();
      for (final ResponsiveStoreRegistration storeRegistration
          : storeRegistry.getRegisteredStoresForChangelog(p)) {
        storeRegistration.onCommit().accept(e.getValue());
      }
    }
    for (final var e : writtenOffsets.entrySet()) {
      final TopicPartition p = e.getKey();
      for (final ResponsiveStoreRegistration storeRegistration
          : storeRegistry.getRegisteredStoresForChangelog(p)) {
        storeRegistration.onCommit().accept(e.getValue());
      }
    }
  }
}
