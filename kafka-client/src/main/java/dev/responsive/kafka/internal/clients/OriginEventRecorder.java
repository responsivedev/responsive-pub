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

import com.google.errorprone.annotations.concurrent.GuardedBy;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OriginEventRecorder implements ResponsiveConsumer.Listener {

  private static final Logger LOG = LoggerFactory.getLogger(OriginEventRecorder.class);
  private static final Duration REPORT_INTERVAL = Duration.ofSeconds(10);

  static final String ORIGIN_EVENT_HEADER_KEY = "_roe";
  static final byte[] ORIGIN_EVENT_MARK = new byte[] {0x1};

  private final BiConsumer<Map<TopicPartition, Long>, String> reportOriginEvents;
  private final String threadId;

  @GuardedBy("this")
  private final Map<TopicPartition, Long> originEvents = new HashMap<>();
  @GuardedBy("this")
  private long lastCommitTs = 0L;

  public OriginEventRecorder(final String threadId) {
    this(threadId, OriginEventRecorder::reportOriginEvents);
  }

  OriginEventRecorder(
      final String threadId,
      final BiConsumer<Map<TopicPartition, Long>, String> reportOriginEvents
  ) {
    this.threadId = threadId;
    this.reportOriginEvents = reportOriginEvents;
  }

  private synchronized void inc(final TopicPartition tp) {
    originEvents.compute(tp, (k, v) -> v == null ? 1 : v + 1);
  }

  @Override
  public <K, V> ConsumerRecords<K, V> onPoll(final ConsumerRecords<K, V> records) {
    for (final ConsumerRecord<K, V> record : records) {
      final var header = record.headers().lastHeader(ORIGIN_EVENT_HEADER_KEY);
      if (header == null) {
        record.headers().add(ORIGIN_EVENT_HEADER_KEY, ORIGIN_EVENT_MARK);
        inc(new TopicPartition(record.topic(), record.partition()));
      }
    }
    return records;
  }

  @Override
  public void onCommit(final Map<TopicPartition, OffsetAndMetadata> offsets) {
    final var report = new HashMap<TopicPartition, Long>();

    synchronized (this) {
      final var now = System.currentTimeMillis();
      final var timeSinceReport = Duration.ofMillis(now - lastCommitTs);
      if (timeSinceReport.compareTo(REPORT_INTERVAL) > 0) {
        lastCommitTs = now;
        for (var tp : offsets.keySet()) {
          final var originCount = originEvents.remove(tp);
          if (originCount != null) {
            report.put(tp, originCount);
          }
        }
      }
    }

    reportOriginEvents.accept(report, threadId);
  }

  private static void reportOriginEvents(
      final Map<TopicPartition, Long> report,
      final String threadId
  ) {
    // TODO(agavra): send this report somewhere instead of just logging it
    LOG.debug("Reporting origin events for {} on {}", report, threadId);
  }

  @Override
  public synchronized void onPartitionsLost(final Collection<TopicPartition> partitions) {
    partitions.forEach(originEvents::remove);
  }

  @Override
  public synchronized void onUnsubscribe() {
    originEvents.clear();
  }
}
