package dev.responsive.kafka.clients;

import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.util.List;
import java.util.OptionalLong;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)

public class ResponsiveRestoreConsumerTest {
  private static final TopicPartition TOPIC_PARTITION1 = new TopicPartition("blue", 1);
  private static final TopicPartition TOPIC_PARTITION2 = new TopicPartition("green", 2);
  private static final TopicPartition TOPIC_PARTITION_NOT_CACHED = new TopicPartition("foo", 3);
  @Mock
  private Consumer<?, ?> wrapped;

  private ResponsiveRestoreConsumer<?, ?> restoreConsumer;

  @BeforeEach
  public void setup() {
    restoreConsumer = new ResponsiveRestoreConsumer<>(
        wrapped,
        tp -> {
          if (tp.equals(TOPIC_PARTITION1)) {
            return OptionalLong.of(123L);
          }
          if (tp.equals(TOPIC_PARTITION2)) {
            return OptionalLong.of(456L);
          }
          return OptionalLong.empty();
        });
  }

  @Test
  public void shouldSetPositionOnAssign() {
    // when:
    restoreConsumer.assign(List.of(TOPIC_PARTITION1, TOPIC_PARTITION2));

    // then:
    verify(wrapped).seek(TOPIC_PARTITION1, 123L);
    verify(wrapped).seek(TOPIC_PARTITION2, 456L);
  }

  @Test
  public void shouldNotSetPositionOnAssignPartitionWithNoCachedPosition() {
    // when:
    restoreConsumer.assign(List.of(TOPIC_PARTITION_NOT_CACHED));

    // then:
    verify(wrapped).assign(List.of(TOPIC_PARTITION_NOT_CACHED));
    verifyNoMoreInteractions(wrapped);
  }

  @Test
  public void shouldApplyFloorOnSeek() {
    // when:
    restoreConsumer.assign(List.of(TOPIC_PARTITION1));
    clearConsumerInvocations();
    restoreConsumer.seek(TOPIC_PARTITION1, 10L);

    // then:
    verify(wrapped).seek(TOPIC_PARTITION1, 123L);
    verifyNoMoreInteractions(wrapped);
  }

  @Test
  public void shouldApplyFloorOnSeekWithMetadata() {
    // when:
    restoreConsumer.assign(List.of(TOPIC_PARTITION1));
    clearConsumerInvocations();
    restoreConsumer.seek(TOPIC_PARTITION1, new OffsetAndMetadata(10L, "bla"));

    // then:
    verify(wrapped).seek(TOPIC_PARTITION1, new OffsetAndMetadata(123L, "bla"));
    verifyNoMoreInteractions(wrapped);
  }

  @Test
  public void shouldApplyFloorOnSeekToBeginning() {
    // when:
    restoreConsumer.assign(List.of(TOPIC_PARTITION1));
    clearConsumerInvocations();
    restoreConsumer.seekToBeginning(List.of(TOPIC_PARTITION1));

    // then:
    verify(wrapped).seek(TOPIC_PARTITION1, 123L);
    verifyNoMoreInteractions(wrapped);
  }

  @Test
  public void shouldNotApplyFloorForPartitionWithNoCachedPosition() {
    // when:
    restoreConsumer.assign(List.of(TOPIC_PARTITION_NOT_CACHED));
    clearConsumerInvocations();
    restoreConsumer.seek(TOPIC_PARTITION_NOT_CACHED, 10L);

    // then:
    verify(wrapped).seek(TOPIC_PARTITION_NOT_CACHED, 10L);
    verifyNoMoreInteractions(wrapped);
  }

  @Test
  public void shouldNotApplyFloorOnSeekToBeginningForPartitionWithNoCachedPosition() {
    // when:
    restoreConsumer.assign(List.of(TOPIC_PARTITION_NOT_CACHED));
    clearConsumerInvocations();
    restoreConsumer.seekToBeginning(List.of(TOPIC_PARTITION_NOT_CACHED));

    // then:
    verify(wrapped).seekToBeginning(List.of(TOPIC_PARTITION_NOT_CACHED));
    verifyNoMoreInteractions(wrapped);
  }

  @SuppressWarnings("unchecked")
  private void clearConsumerInvocations() {
    clearInvocations(wrapped);
  }
}
