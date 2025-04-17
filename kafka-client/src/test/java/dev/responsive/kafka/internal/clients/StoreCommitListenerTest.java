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

package dev.responsive.kafka.internal.clients;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import dev.responsive.kafka.internal.stores.ResponsiveStoreRegistration;
import dev.responsive.kafka.internal.stores.ResponsiveStoreRegistry;
import java.util.Map;
import java.util.Map.Entry;
import java.util.OptionalLong;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class StoreCommitListenerTest {
  private static final TopicPartition PARTITION1 = new TopicPartition("yogi", 1);
  private static final TopicPartition PARTITION2 = new TopicPartition("booboo", 2);

  @Mock
  private ResponsiveStoreRegistration.StoreCallbacks store1Callbacks;
  @Mock
  private ResponsiveStoreRegistration.StoreCallbacks store2Callbacks;
  private final ResponsiveStoreRegistry registry = new ResponsiveStoreRegistry();
  private final OffsetRecorder offsetRecorder = new OffsetRecorder(true, "thread1");
  private StoreCommitListener commitListener;

  @BeforeEach
  public void setup() {
    registry.registerStore(new ResponsiveStoreRegistration(
        "store1",
        PARTITION1,
        OptionalLong.of(0),
        store1Callbacks,
        "thread1"
    ));
    registry.registerStore(new ResponsiveStoreRegistration(
        "store2",
        PARTITION2,
        OptionalLong.of(0),
        store2Callbacks,
        "thread1"
    ));
    commitListener = new StoreCommitListener(registry, offsetRecorder);
  }

  @Test
  public void shouldNotifyStoresOnCommittedChangelogOffsets() {
    // when:
    sendCommittedOffsets(Map.of(PARTITION1, 123L));

    // then:
    verify(store1Callbacks).notifyCommit(123L);
    verifyNoInteractions(store2Callbacks);
  }

  @Test
  public void shouldNotifyStoresOnCommittedChangelogWrites() {
    // when:
    sendWrittenOffsets(Map.of(PARTITION2, 456L));

    // then:
    verify(store2Callbacks).notifyCommit(456L);
    verifyNoInteractions(store1Callbacks);
  }

  private void sendCommittedOffsets(final Map<TopicPartition, Long> offsets) {
    offsetRecorder.getProducerListener().onSendOffsetsToTransaction(
        offsets.entrySet().stream()
            .collect(Collectors.toMap(Entry::getKey, e -> new OffsetAndMetadata(e.getValue()))),
        "group"
    );
    offsetRecorder.getProducerListener().onProducerCommit();
  }

  private void sendWrittenOffsets(final Map<TopicPartition, Long> offsets) {
    for (final var e : offsets.entrySet()) {
      offsetRecorder.getProducerListener().onSendCompleted(new RecordMetadata(
          e.getKey(),
          e.getValue(),
          0,
          0,
          0,
          0
      ));
    }
    offsetRecorder.getProducerListener().onProducerCommit();
  }
}