/*
 * Copyright 2024 Responsive Computing, Inc.
 *
 * This source code is licensed under the Responsive Software License Agreement v1.0 available at
 *
 * https://www.responsive.dev/legal/responsive-bsl-10
 *
 * This software requires a valid Commercial License Key for production use. Trial and commercial
 * licenses can be obtained at https://www.responsive.dev/sdk/get-started.
 */

package dev.responsive.kafka.internal.clients;

import dev.responsive.kafka.internal.clients.OffsetRecorder.RecordingKey;
import dev.responsive.kafka.internal.stores.ResponsiveStoreRegistration;
import dev.responsive.kafka.internal.stores.ResponsiveStoreRegistry;
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
      final String threadId,
      final Map<RecordingKey, Long> committedOffsets,
      final Map<TopicPartition, Long> writtenOffsets
  ) {
    // TODO: this is kind of inefficient (iterates over stores a lot). Not a huge deal
    //       as we only run this on each commit. Can fix by indexing stores in registry
    for (final var e : committedOffsets.entrySet()) {
      final TopicPartition p = e.getKey().getPartition();
      for (final ResponsiveStoreRegistration storeRegistration
          : storeRegistry.getRegisteredStoresForChangelog(p, threadId)) {
        storeRegistration.onCommit().accept(e.getValue());
      }
    }
    for (final var e : writtenOffsets.entrySet()) {
      final TopicPartition p = e.getKey();
      for (final ResponsiveStoreRegistration storeRegistration
          : storeRegistry.getRegisteredStoresForChangelog(p, threadId)) {
        storeRegistration.onCommit().accept(e.getValue());
      }
    }
  }
}
