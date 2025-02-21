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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

import dev.responsive.kafka.internal.clients.OffsetRecorder.RecordingKey;
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
  private String threadId;
  private final OffsetRecorder eosRecorder = new OffsetRecorder(true, "thread");
  private final OffsetRecorder alosRecorder = new OffsetRecorder(false, "thread");

  @BeforeEach
  public void setup() {
    eosRecorder.addCommitCallback((t, c, w) -> {
      committedOffsets = c;
      writtenOffsets = w;
      threadId = t;
    });
    alosRecorder.addCommitCallback((t, c, w) -> {
      committedOffsets = c;
      writtenOffsets = w;
      threadId = t;
    });
  }

  @Test
  public void shouldRecordTransactionalOffsets() {
    // given:
    eosRecorder.getProducerListener().onSendOffsetsToTransaction(
        Map.of(
            TOPIC_PARTITION1, new OffsetAndMetadata(123L),
            TOPIC_PARTITION2, new OffsetAndMetadata(456L)
        ),
        "group"
    );

    // when:
    eosRecorder.getProducerListener().onProducerCommit();

    // then:
    assertThat(getCommittedOffsetsSentToCallback(), is(Map.of(
        new RecordingKey(TOPIC_PARTITION1, "group"), 123L,
        new RecordingKey(TOPIC_PARTITION2, "group"), 456L
    )));
    assertThat(getThreadSentToCallback(), is("thread"));
  }

  @Test
  public void shouldRecordProducedOffsets() {
    // given:
    eosRecorder.getProducerListener().onSendCompleted(
        new RecordMetadata(TOPIC_PARTITION1, 123L, 0, 0, 0, 0)
    );
    eosRecorder.getProducerListener().onSendCompleted(
        new RecordMetadata(TOPIC_PARTITION2, 456L, 0, 0, 0, 0)
    );

    // when:
    eosRecorder.getProducerListener().onProducerCommit();

    // then:
    assertThat(getWrittenOffsetsSentToCallback(), is(Map.of(
        TOPIC_PARTITION1, 123L,
        TOPIC_PARTITION2, 456L
    )));
    assertThat(getThreadSentToCallback(), is("thread"));
  }

  @Test
  public void shouldResetOffsetsOnAbort() {
    // given:
    eosRecorder.getProducerListener().onSendOffsetsToTransaction(
        Map.of(
            TOPIC_PARTITION1, new OffsetAndMetadata(123L),
            TOPIC_PARTITION2, new OffsetAndMetadata(456L)
        ),
        "group"
    );
    eosRecorder.getProducerListener().onSendCompleted(
        new RecordMetadata(TOPIC_PARTITION1, 123L, 0, 0, 0, 0)
    );
    eosRecorder.getProducerListener().onSendCompleted(
        new RecordMetadata(TOPIC_PARTITION2, 456L, 0, 0, 0, 0)
    );

    // when:
    eosRecorder.getProducerListener().onAbort();
    eosRecorder.getProducerListener().onProducerCommit();

    // then:
    assertThat(getWrittenOffsetsSentToCallback().entrySet(), is(empty()));
    assertThat(getCommittedOffsetsSentToCallback().entrySet(), is(empty()));
  }

  @Test
  public void shouldRecordOffsetsCommittedByConsumer() {
    // when:
    alosRecorder.getConsumerListener().onConsumerCommit(
        Map.of(
            TOPIC_PARTITION1, new OffsetAndMetadata(123L),
            TOPIC_PARTITION2, new OffsetAndMetadata(456L)
        )
    );

    // then:
    assertThat(getCommittedOffsetsSentToCallback(), is(Map.of(
        new RecordingKey(TOPIC_PARTITION1, ""), 123L,
        new RecordingKey(TOPIC_PARTITION2, ""), 456L
    )));
    assertThat(getThreadSentToCallback(), is("thread"));
  }

  @Test
  public void shouldThrowIfCommitFromConsumerOnEos() {
    assertThrows(
        IllegalStateException.class,
        () -> eosRecorder.getConsumerListener().onConsumerCommit(Map.of())
    );
  }

  @Test
  public void shouldThrowIfTransactionalOffsetsSentOnAlos() {
    assertThrows(
        IllegalStateException.class,
        () -> alosRecorder.getProducerListener().onSendOffsetsToTransaction(Map.of(), "")
    );
  }

  private Map<RecordingKey, Long> getCommittedOffsetsSentToCallback() {
    return committedOffsets;
  }

  private Map<TopicPartition, Long> getWrittenOffsetsSentToCallback() {
    return writtenOffsets;
  }

  private String getThreadSentToCallback() {
    return threadId;
  }
}