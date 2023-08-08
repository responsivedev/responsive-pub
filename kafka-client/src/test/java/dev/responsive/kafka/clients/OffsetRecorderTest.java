package dev.responsive.kafka.clients;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

import dev.responsive.kafka.clients.OffsetRecorder.RecordingKey;
import java.util.Map;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class OffsetRecorderTest {
  private static final TopicPartition TOPIC_PARTITION1 = new TopicPartition("yogi", 1);
  private static final TopicPartition TOPIC_PARTITION2 = new TopicPartition("bear", 2);

  private Map<RecordingKey, Long> committedOffsets;
  private Map<TopicPartition, Long> writtenOffsets;
  private final OffsetRecorder eosRecorder = new OffsetRecorder(true);
  private final OffsetRecorder alosRecorder = new OffsetRecorder(false);

  @BeforeEach
  public void setup() {
    eosRecorder.addCommitCallback(
        (c, w) -> {
          committedOffsets = c;
          writtenOffsets = w;
        });
    alosRecorder.addCommitCallback(
        (c, w) -> {
          committedOffsets = c;
          writtenOffsets = w;
        });
  }

  @Test
  public void shouldRecordTransactionalOffsets() {
    // given:
    eosRecorder
        .getProducerListener()
        .onSendOffsetsToTransaction(
            Map.of(
                TOPIC_PARTITION1, new OffsetAndMetadata(123L),
                TOPIC_PARTITION2, new OffsetAndMetadata(456L)),
            "group");

    // when:
    eosRecorder.getProducerListener().onCommit();

    // then:
    assertThat(
        getCommittedOffsetsSentToCallback(),
        is(
            Map.of(
                new RecordingKey(TOPIC_PARTITION1, "group"), 123L,
                new RecordingKey(TOPIC_PARTITION2, "group"), 456L)));
  }

  @Test
  public void shouldRecordProducedOffsets() {
    // given:
    eosRecorder
        .getProducerListener()
        .onSendCompleted(new RecordMetadata(TOPIC_PARTITION1, 123L, 0, 0, 0, 0));
    eosRecorder
        .getProducerListener()
        .onSendCompleted(new RecordMetadata(TOPIC_PARTITION2, 456L, 0, 0, 0, 0));

    // when:
    eosRecorder.getProducerListener().onCommit();

    // then:
    assertThat(
        getWrittenOffsetsSentToCallback(),
        is(
            Map.of(
                TOPIC_PARTITION1, 123L,
                TOPIC_PARTITION2, 456L)));
  }

  @Test
  public void shouldResetOffsetsOnAbort() {
    // given:
    eosRecorder
        .getProducerListener()
        .onSendOffsetsToTransaction(
            Map.of(
                TOPIC_PARTITION1, new OffsetAndMetadata(123L),
                TOPIC_PARTITION2, new OffsetAndMetadata(456L)),
            "group");
    eosRecorder
        .getProducerListener()
        .onSendCompleted(new RecordMetadata(TOPIC_PARTITION1, 123L, 0, 0, 0, 0));
    eosRecorder
        .getProducerListener()
        .onSendCompleted(new RecordMetadata(TOPIC_PARTITION2, 456L, 0, 0, 0, 0));

    // when:
    eosRecorder.getProducerListener().onAbort();
    eosRecorder.getProducerListener().onCommit();

    // then:
    assertThat(getWrittenOffsetsSentToCallback().entrySet(), is(empty()));
    assertThat(getCommittedOffsetsSentToCallback().entrySet(), is(empty()));
  }

  @Test
  public void shouldRecordOffsetsCommittedByConsumer() {
    // when:
    alosRecorder
        .getConsumerListener()
        .onCommit(
            Map.of(
                TOPIC_PARTITION1, new OffsetAndMetadata(123L),
                TOPIC_PARTITION2, new OffsetAndMetadata(456L)));

    // then:
    assertThat(
        getCommittedOffsetsSentToCallback(),
        is(
            Map.of(
                new RecordingKey(TOPIC_PARTITION1, ""), 123L,
                new RecordingKey(TOPIC_PARTITION2, ""), 456L)));
  }

  @Test
  public void shouldThrowIfCommitFromConsumerOnEos() {
    assertThrows(
        IllegalStateException.class, () -> eosRecorder.getConsumerListener().onCommit(Map.of()));
  }

  @Test
  public void shouldThrowIfTransactionalOffsetsSentOnAlos() {
    assertThrows(
        IllegalStateException.class,
        () -> alosRecorder.getProducerListener().onSendOffsetsToTransaction(Map.of(), ""));
  }

  private Map<RecordingKey, Long> getCommittedOffsetsSentToCallback() {
    return committedOffsets;
  }

  private Map<TopicPartition, Long> getWrittenOffsetsSentToCallback() {
    return writtenOffsets;
  }
}
