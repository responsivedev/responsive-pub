package dev.responsive.kafka.internal.clients;

import dev.responsive.kafka.internal.clients.OffsetRecorder.RecordingKey;
import dev.responsive.kafka.internal.stores.ResponsiveStoreRegistration;
import dev.responsive.kafka.internal.stores.ResponsiveStoreRegistry;
import dev.responsive.kafka.internal.utils.UrandomGenerator;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StoreCommitListener {
  private static final Logger LOG = LoggerFactory.getLogger(StoreCommitListener.class);

  private final ResponsiveStoreRegistry storeRegistry;
  private final UrandomGenerator generator = new UrandomGenerator();

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
        if (Math.abs(generator.nextLong()) % 200 < 1) {
          LOG.info("inject sleep in store commit (off)");
          try {
            Thread.sleep(35000);
          } catch (final InterruptedException ex) {
            // ignore
          }
        }
        storeRegistration.onCommit().accept(e.getValue());
      }
    }
    for (final var e : writtenOffsets.entrySet()) {
      final TopicPartition p = e.getKey();
      for (final ResponsiveStoreRegistration storeRegistration
          : storeRegistry.getRegisteredStoresForChangelog(p)) {
        if (Math.abs(generator.nextLong()) % 200 < 1) {
          LOG.info("inject sleep in store commit");
          try {
            Thread.sleep(35000);
          } catch (final InterruptedException ex) {
            // ignore
          }
        }
        storeRegistration.onCommit().accept(e.getValue());
      }
    }
  }
}
