package dev.responsive.kafka.internal.clients;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
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
    restoreConsumer = getRestoreConsumer(false);
  }

  private ResponsiveRestoreConsumer<?, ?> getRestoreConsumer(boolean repairOffsets) {
    return  new ResponsiveRestoreConsumer<>(
        "restore-consumer",
        wrapped,
        tp -> {
          if (tp.equals(TOPIC_PARTITION1)) {
            return OptionalLong.of(123L);
          }
          if (tp.equals(TOPIC_PARTITION2)) {
            return OptionalLong.of(456L);
          }
          return OptionalLong.empty();
        },
        repairOffsets
    );
  }

  @Test
  public void shouldNotResetPositionOnAssignWithCachedPosition() {
    // when:
    restoreConsumer.assign(List.of(TOPIC_PARTITION1, TOPIC_PARTITION2));

    // then:
    verify(wrapped, times(2)).assignment();
    verify(wrapped).assign(List.of(TOPIC_PARTITION1, TOPIC_PARTITION2));
    verifyNoMoreInteractions(wrapped);
  }

  @Test
  public void shouldNotSetPositionOnAssignPartitionWithNoCachedPosition() {
    // when:
    restoreConsumer.assign(List.of(TOPIC_PARTITION_NOT_CACHED));

    // then:
    verify(wrapped).assignment();
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
    restoreConsumer.seekToBeginning(Set.of(TOPIC_PARTITION_NOT_CACHED));

    // then:
    verify(wrapped).seekToBeginning(Set.of(TOPIC_PARTITION_NOT_CACHED));
    verifyNoMoreInteractions(wrapped);
  }

  @Test
  public void shouldThrowIfCallToPollWithoutSeek() {
    // when:
    restoreConsumer.assign(List.of(TOPIC_PARTITION1, TOPIC_PARTITION2));

    restoreConsumer.seekToBeginning(Collections.singleton(TOPIC_PARTITION1));

    // then:
    Assertions.assertThrows(
        IllegalStateException.class,
        () -> restoreConsumer.poll(Duration.ofMillis(100))
    );
  }

  @Test
  public void shouldThrowIfCallToPollWithoutSeekAfterReassignment() {
    // when:
    when(wrapped.assignment())
        .thenReturn(Set.of(TOPIC_PARTITION1, TOPIC_PARTITION2))
        .thenReturn(Set.of(TOPIC_PARTITION2));

    restoreConsumer.assign(List.of(TOPIC_PARTITION1, TOPIC_PARTITION2));
    restoreConsumer.seekToBeginning(List.of(TOPIC_PARTITION1, TOPIC_PARTITION2));

    // remove tp1 to clear its seek
    restoreConsumer.assign(List.of(TOPIC_PARTITION2));

    // reassign tp1 requires a new seek before poll
    restoreConsumer.assign(List.of(TOPIC_PARTITION1, TOPIC_PARTITION2));

    // then:
    Assertions.assertThrows(
        IllegalStateException.class,
        () -> restoreConsumer.poll(Duration.ofMillis(100))
    );
  }

  @Test
  public void shouldClearUninitializedPartitionsWhenUnassigned() {
    // when:
    restoreConsumer.assign(List.of(TOPIC_PARTITION1, TOPIC_PARTITION2));
    when(wrapped.assignment()).thenReturn(Set.of(TOPIC_PARTITION1, TOPIC_PARTITION2));

    restoreConsumer.seekToBeginning(Collections.singleton(TOPIC_PARTITION1));

    restoreConsumer.assign(List.of(TOPIC_PARTITION1));

    // then:
    restoreConsumer.poll(Duration.ofMillis(100));
  }

  @Test
  public void shouldClearUninitializedPartitionsWhenUnsubscribed() {
    // when:
    restoreConsumer.assign(List.of(TOPIC_PARTITION1, TOPIC_PARTITION2));

    restoreConsumer.unsubscribe();

    // then:
    restoreConsumer.poll(Duration.ofMillis(100));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldSeekToBeginningOnOffsetOutOfRangeIfRepairOffsetConfigured() {
    // Given:
    restoreConsumer = getRestoreConsumer(true);
    final ArgumentCaptor<Collection<TopicPartition>> seekedTps
        = ArgumentCaptor.forClass(Collection.class);

    when(wrapped.poll(any()))
        .thenThrow(new OffsetOutOfRangeException(Map.of(TOPIC_PARTITION1, 1L)))
        .thenReturn(null);
    doNothing().when(wrapped).seekToBeginning(seekedTps.capture());

    // When:
    restoreConsumer.assign(List.of(TOPIC_PARTITION1));
    restoreConsumer.seekToBeginning(List.of(TOPIC_PARTITION1));
    restoreConsumer.poll(Duration.ofMillis(100));

    // Then:
    verify(wrapped, Mockito.times(2)).poll(any());
    verify(wrapped).seekToBeginning(Set.of(TOPIC_PARTITION1));
  }

  @Test
  public void shouldThrowOffsetOutOfRangeExceptionIfNoRepairConfigured() {
    // Given:
    when(wrapped.poll(any()))
        .thenThrow(new OffsetOutOfRangeException(Map.of(TOPIC_PARTITION1, 1L)))
        .thenReturn(null);

    // Then:
    Assertions.assertThrows(
        OffsetOutOfRangeException.class,
        () -> restoreConsumer.poll(Duration.ofMillis(100))
    );
  }

  @SuppressWarnings("unchecked")
  private void clearConsumerInvocations() {
    clearInvocations(wrapped);
  }
}
