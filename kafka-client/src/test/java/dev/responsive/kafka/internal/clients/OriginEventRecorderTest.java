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

import static dev.responsive.kafka.internal.clients.OriginEventRecorder.ORIGIN_EVENT_HEADER_KEY;
import static dev.responsive.kafka.internal.clients.OriginEventRecorder.ORIGIN_EVENT_MARK;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

class OriginEventRecorderTest {

  private static final TopicPartition TOPIC_PARTITION = new TopicPartition("topic", 0);

  @Test
  public void shouldReturnInputRecordsWithIncrementeHeaderOnNewOriginEvent() {
    // Given:
    final var oeRecorder = new OriginEventRecorder("thread");
    final var records = new ConsumerRecords<>(Map.of(
        TOPIC_PARTITION, List.of(from(null))
    ));

    // When:
    final var result = oeRecorder
        .onPoll(records)
        .records(TOPIC_PARTITION.topic())
        .iterator();

    // Then:
    assertThat(
        result.next().headers().lastHeader(ORIGIN_EVENT_HEADER_KEY).value(),
        Matchers.is(ORIGIN_EVENT_MARK)
    );
  }

  @Test
  public void shouldReportOneWhenCommittingNewOriginEvent() {
    // Given:
    final var seenOrigins = new AtomicLong(0L);
    final var oeRecorder = new OriginEventRecorder("thread", (res, thread) -> {
      seenOrigins.addAndGet(res.getOrDefault(TOPIC_PARTITION, 0L));
    });
    final var records = new ConsumerRecords<>(Map.of(
        TOPIC_PARTITION, List.of(from(null), from(null))
    ));

    // When:
    oeRecorder.onPoll(records);
    oeRecorder.onCommit(Map.of(TOPIC_PARTITION, new OffsetAndMetadata(0L)));

    // Then:
    assertThat(seenOrigins.get(), Matchers.is(2L));
  }

  @Test
  public void shouldNotReportOneWhenCommittingNonOriginEvent() {
    // Given:
    final var seenOrigins = new AtomicLong(0L);
    final var oeRecorder = new OriginEventRecorder("thread", (res, thread) -> {
      seenOrigins.addAndGet(res.getOrDefault(TOPIC_PARTITION, 0L));
    });
    final var records = new ConsumerRecords<>(Map.of(
        TOPIC_PARTITION, List.of(from(ORIGIN_EVENT_MARK))
    ));

    // When:
    oeRecorder.onPoll(records);
    oeRecorder.onCommit(Map.of(TOPIC_PARTITION, new OffsetAndMetadata(0L)));

    // Then:
    assertThat(seenOrigins.get(), Matchers.is(0L));
  }

  @Test
  public void shouldNotChangeOriginEventHeaderKey() {
    assertThat(ORIGIN_EVENT_HEADER_KEY, Matchers.is("_roe"));
    assertThat(ORIGIN_EVENT_MARK, Matchers.is(new byte[] {0x1}));
  }

  private static ConsumerRecord<String, String> from(final byte[] oeHeader) {
    final var record = new ConsumerRecord<>(TOPIC_PARTITION.topic(),
        TOPIC_PARTITION.partition(),
        0L,
        "foo",
        "bar"
    );

    if (oeHeader != null) {
      record.headers().add(ORIGIN_EVENT_HEADER_KEY, oeHeader);
    }

    return record;
  }

}