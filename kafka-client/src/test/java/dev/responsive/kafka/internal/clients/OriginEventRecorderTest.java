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

import static dev.responsive.kafka.internal.clients.OriginEventRecorderImpl.ORIGIN_EVENT_HEADER_KEY;
import static dev.responsive.kafka.internal.clients.OriginEventRecorderImpl.ORIGIN_EVENT_MARK;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class OriginEventRecorderTest {

  private static final TopicPartition TP = new TopicPartition("topic", 0);

  private OriginEventRecorderImpl recorder;
  private TestReporter reporter;

  @BeforeEach
  public void setup() {
    reporter = new TestReporter();
    recorder = new OriginEventRecorderImpl("test-thread", reporter, false);
  }

  @Test
  public void shouldTrackOffsetAndReportOnConsumerCommit() {
    // Given:
    final long offset = 1000L;
    final ConsumerRecord<String, String> record = record(offset, false);
    final ConsumerRecords<String, String> records = records(record);

    // When:
    recorder.onPoll(records);
    recorder.onConsumerCommit(Map.of(TP, new OffsetAndMetadata(offset + 1)));

    // Then: a report is generated with a count of 1 for the partition.
    List<ReportCall> calls = reporter.getCalls();
    assertThat("A report should have been triggered", calls, hasSize(1));

    ReportCall call = calls.get(0);
    assertThat("Report thread id should be 'test-thread'", call.threadId, is("test-thread"));
    assertThat("Report should contain the TopicPartition", call.report.keySet(), hasItem(TP));
    assertThat("Count for the partition should be 1", call.report.get(TP), is(1));
  }

  @Test
  public void shouldNotTrackOffsetWhenOriginHeaderPresent() {
    // Given:
    long offset = 1000L;
    ConsumerRecords<String, String> records = records(
        record(offset, true),
        record(offset + 1, false)
    );

    // When:
    recorder.onPoll(records);
    Map<TopicPartition, OffsetAndMetadata> commitOffsets = new HashMap<>();
    commitOffsets.put(TP, new OffsetAndMetadata(offset + 2));
    recorder.onConsumerCommit(commitOffsets);

    // Then:
    List<ReportCall> calls = reporter.getCalls();
    assertThat("A report should have been triggered", calls, hasSize(1));

    ReportCall call = calls.get(0);
    assertThat("Report thread id should be 'test-thread'", call.threadId, is("test-thread"));
    assertThat("Report should contain the TopicPartition", call.report.keySet(), hasItem(TP));
    assertThat("Count for the partition should only count 1", call.report.get(TP), is(1));
  }

  @Test
  public void shouldAddOriginHeaderOnSend() {
    // Given:
    ProducerRecord<String, String> record = new ProducerRecord<>(
        TP.topic(), TP.partition(), null, "key", "value"
    );

    // When:
    ProducerRecord<String, String> returnedRecord = recorder.onSend(record);

    // Then:
    assertThat("Record should have origin header after onSend",
        returnedRecord.headers().lastHeader(ORIGIN_EVENT_HEADER_KEY).value(),
        is(ORIGIN_EVENT_MARK)
    );
  }

  @Test
  public void shouldReportOnProducerCommitWithEos() {
    // Given:
    recorder = new OriginEventRecorderImpl("test-thread", reporter, true);

    long offset = 1000L;
    final ConsumerRecords<String, String> records = records(record(offset, false));
    recorder.onPoll(records);

    // When:
    recorder.onSendOffsetsToTransaction(Map.of(TP, new OffsetAndMetadata(offset + 1)), "");
    recorder.onProducerCommit();

    // Then: a report is triggered with the correct count.
    List<ReportCall> calls = reporter.getCalls();
    assertThat("A report should be triggered on producer commit", calls, hasSize(1));
    ReportCall call = calls.get(0);
    assertThat("Reported count for the partition should be 1", call.report.get(TP), is(1));
  }

  @Test
  public void shouldOnlyIncludeOffsetsUpToCommitOffset() {
    // Given:
    long baseOffset = 1000L;
    recorder.onPoll(records(
        record(baseOffset, false),
        record(baseOffset + 1, false)
    ));

    // When:
    recorder.onConsumerCommit(Map.of(TP, new OffsetAndMetadata(baseOffset + 1)));

    // Then: only the mark for offset 1000 (i.e. below commit offset 5001) is counted.
    final List<ReportCall> calls = reporter.getCalls();
    assertThat("A report should be triggered", calls, hasSize(1));
    assertThat(
        "Reported count for the partition should be 1 (only offset 1000 counted)",
        calls.get(0).report.get(TP),
        is(1)
    );
  }

  @Test
  public void shouldOnlyReportCommittedTopicPartitions() {
    // Given:
    final TopicPartition tp1 = new TopicPartition("t1", 0);
    final TopicPartition tp2 = new TopicPartition("t2", 0);
    long offset = 1000L;

    final var record1 = new ConsumerRecord<>(tp1.topic(), tp1.partition(), offset, "key", "value");
    final var record2 = new ConsumerRecord<>(tp2.topic(), tp2.partition(), offset, "key", "value");

    // When:
    recorder.onPoll(records(record1, record2));
    recorder.onConsumerCommit(Map.of(tp1, new OffsetAndMetadata(offset + 1)));

    // Then:
    List<ReportCall> calls = reporter.getCalls();
    assertThat("A report should have been triggered", calls, hasSize(1));
    ReportCall call = calls.get(0);
    assertThat("Report should only contain the committed TopicPartition",
        call.report.keySet(), contains(tp1));
    assertThat("Reported count for tp1 should be 1", call.report.get(tp1), is(1));
  }


  private ConsumerRecord<String, String> record(
      final long offset,
      final boolean withOriginHeader
  ) {
    final ConsumerRecord<String, String> record = new ConsumerRecord<>(
        TP.topic(),
        TP.partition(),
        offset,
        "key",
        "value"
    );

    if (withOriginHeader) {
      record.headers().add(ORIGIN_EVENT_HEADER_KEY, ORIGIN_EVENT_MARK);
    }

    return record;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private ConsumerRecords<String, String> records(final ConsumerRecord... records) {
    return new ConsumerRecords<>(Map.of(TP, Arrays.asList(records)));
  }

  private static class ReportCall {
    final Map<TopicPartition, Integer> report;
    final String threadId;

    ReportCall(Map<TopicPartition, Integer> report, String threadId) {
      this.report = report;
      this.threadId = threadId;
    }
  }

  private static class TestReporter
      implements java.util.function.BiConsumer<Map<TopicPartition, Integer>, String> {

    private final List<ReportCall> calls = new ArrayList<>();

    @Override
    public void accept(Map<TopicPartition, Integer> report, String threadId) {
      calls.add(new ReportCall(report, threadId));
    }

    public List<ReportCall> getCalls() {
      return calls;
    }
  }
}
