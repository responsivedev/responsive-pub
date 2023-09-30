package dev.responsive.kafka.internal.clients;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import dev.responsive.kafka.internal.clients.OffsetRecorder;
import dev.responsive.kafka.internal.clients.StoreCommitListener;
import dev.responsive.kafka.internal.stores.ResponsiveStoreRegistration;
import dev.responsive.kafka.internal.stores.ResponsiveStoreRegistry;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Consumer;
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
  private Consumer<Long> store1Flush;
  @Mock
  private Consumer<Long> store2Flush;
  private final ResponsiveStoreRegistry registry = new ResponsiveStoreRegistry();
  private final OffsetRecorder offsetRecorder = new OffsetRecorder(true);
  private StoreCommitListener commitListener;

  @BeforeEach
  public void setup() {
    registry.registerStore(new ResponsiveStoreRegistration(
        "store1",
        PARTITION1,
        0,
        store1Flush
    ));
    registry.registerStore(new ResponsiveStoreRegistration(
        "store2",
        PARTITION2,
        0,
        store2Flush
    ));
    commitListener = new StoreCommitListener(registry, offsetRecorder);
  }

  @Test
  public void shouldNotifyStoresOnCommittedChangelogOffsets() {
    // when:
    sendCommittedOffsets(Map.of(PARTITION1, 123L));

    // then:
    verify(store1Flush).accept(123L);
    verifyNoInteractions(store2Flush);
  }

  @Test
  public void shouldNotifyStoresOnCommittedChangelogWrites() {
    // when:
    sendWrittenOffsets(Map.of(PARTITION2, 456L));

    // then:
    verify(store2Flush).accept(456L);
    verifyNoInteractions(store1Flush);
  }

  private void sendCommittedOffsets(final Map<TopicPartition, Long> offsets) {
    offsetRecorder.getProducerListener().onSendOffsetsToTransaction(
        offsets.entrySet().stream()
            .collect(Collectors.toMap(Entry::getKey, e -> new OffsetAndMetadata(e.getValue()))),
        "group"
    );
    offsetRecorder.getProducerListener().onCommit();
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
    offsetRecorder.getProducerListener().onCommit();
  }
}