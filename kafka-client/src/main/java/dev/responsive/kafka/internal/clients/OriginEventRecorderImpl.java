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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import dev.responsive.kafka.api.config.ResponsiveConfig;
import dev.responsive.kafka.internal.license.model.LicenseInfo;
import dev.responsive.kafka.internal.license.model.UsageBasedV1;
import dev.responsive.kafka.internal.license.server.model.OriginEventsReportRequestV1;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OriginEventRecorderImpl implements OriginEventRecorder {

  private static final Logger LOG = LoggerFactory.getLogger(OriginEventRecorderImpl.class);

  static final String ORIGIN_EVENT_HEADER_KEY = "_roe";
  static final byte[] ORIGIN_EVENT_MARK = new byte[] {0x1};

  private final Duration reportInterval;
  private final BiConsumer<Map<TopicPartition, Integer>, String> reportOriginEvents;
  private final String threadId;
  private final boolean eos;

  @GuardedBy("this")
  private final Map<TopicPartition, OffsetTracker> trackedOffsets = new HashMap<>();
  @GuardedBy("this")
  private final Map<TopicPartition, OffsetAndMetadata> nextCommitOffsets = new HashMap<>();
  @GuardedBy("this")
  private final Map<TopicPartition, Integer> ongoingReport = new HashMap<>();
  @GuardedBy("this")
  private long lastReportTs;

  public OriginEventRecorderImpl(
      final String threadId,
      final ResponsiveConfig config,
      final LicenseInfo license,
      final boolean eos
  ) {
    this(
        threadId,
        new Reporter(config, license),
        config.getLong(ResponsiveConfig.ORIGIN_EVENT_REPORT_INTERVAL_MS_CONFIG),
        eos
    );
  }

  @VisibleForTesting
  OriginEventRecorderImpl(
      final String threadId,
      final BiConsumer<Map<TopicPartition, Integer>, String> reportOriginEvents,
      final boolean eos
  ) {
    this(threadId, reportOriginEvents, 0L, eos);
  }

  private OriginEventRecorderImpl(
      final String threadId,
      final BiConsumer<Map<TopicPartition, Integer>, String> reportOriginEvents,
      final long reportIntervalMs,
      final boolean eos
  ) {
    this.threadId = threadId;
    this.reportOriginEvents = reportOriginEvents;
    this.reportInterval = Duration.ofMillis(reportIntervalMs);
    this.lastReportTs = System.currentTimeMillis();
    this.eos = eos;
  }

  @Override
  public <K, V> ConsumerRecords<K, V> onPoll(final ConsumerRecords<K, V> records) {
    for (final ConsumerRecord<K, V> record : records) {
      final var header = record.headers().lastHeader(ORIGIN_EVENT_HEADER_KEY);
      if (header == null) {
        synchronized (this) {
          final var tracker = trackedOffsets.computeIfAbsent(
              new TopicPartition(record.topic(), record.partition()),
              tp -> new OffsetTracker(record.offset())
          );
          tracker.mark(record.offset());
        }
      }
    }
    return records;
  }

  @Override
  public <K, V> ProducerRecord<K, V> onSend(final ProducerRecord<K, V> record) {
    final var header = record.headers().lastHeader(ORIGIN_EVENT_HEADER_KEY);
    if (header == null) {
      record.headers().add(ORIGIN_EVENT_HEADER_KEY, ORIGIN_EVENT_MARK);
    }
    return record;
  }

  @Override
  public void onConsumerCommit(final Map<TopicPartition, OffsetAndMetadata> offsets) {
    if (eos) {
      throw new IllegalStateException("Should not use consumer commit on EOS");
    }

    synchronized (this) {
      nextCommitOffsets.putAll(offsets);
    }
    onCommit();
  }

  @Override
  public void onProducerCommit() {
    if (!eos) {
      throw new IllegalStateException("Should not use producer commit on ALOS");
    }

    onCommit();
  }

  @Override
  public synchronized void onSendOffsetsToTransaction(
      final Map<TopicPartition, OffsetAndMetadata> offsets,
      final String consumerGroupId
  ) {
    nextCommitOffsets.putAll(offsets);
  }

  private void onCommit() {
    Map<TopicPartition, Integer> report = null;

    synchronized (this) {
      for (final var tpOffset : nextCommitOffsets.entrySet()) {
        final var tracker = trackedOffsets.get(tpOffset.getKey());
        if (tracker != null) {
          final int count = tracker.countAndShift(tpOffset.getValue().offset());
          ongoingReport.compute(
              tpOffset.getKey(),
              (k, oldCount) -> oldCount == null ? count : oldCount + count
          );
        }
      }

      nextCommitOffsets.clear();
      final long now = System.currentTimeMillis();
      if (Duration.ofMillis(now - lastReportTs).compareTo(reportInterval) >= 0) {
        report = Map.copyOf(ongoingReport);
        lastReportTs = now;
        ongoingReport.clear();
      }
    }

    // do this outside of the synchronized block since the report may
    // involve a network call
    if (report != null) {
      reportOriginEvents.accept(report, threadId);
    }
  }

  @Override
  public synchronized void onPartitionsLost(final Collection<TopicPartition> partitions) {
    // since we won't get commits for these anymore, we should clear the offsets
    // we have seen so far but not committed in case we ever get the topic partition
    // reassigned back to ourselves
    partitions.forEach(trackedOffsets::remove);
  }

  @Override
  public synchronized void onUnsubscribe() {
    // clear all seen tracked offsets that have not yet been committed
    trackedOffsets.clear();
  }

  private static class Reporter implements BiConsumer<Map<TopicPartition, Integer>, String> {

    private final ObjectMapper mapper = new ObjectMapper();
    private final HttpClient client;
    private final String licenseServer;
    private final String apiKey;
    private final String appId;
    private final String env;

    private Reporter(ResponsiveConfig config, final LicenseInfo license) {
      if (!license.type().equals(UsageBasedV1.TYPE_NAME)) {
        throw new IllegalArgumentException(
            "Invalid license type for usage reporting: " + license.type());
      }

      this.licenseServer = config.getString(ResponsiveConfig.RESPONSIVE_LICENSE_SERVER_CONFIG);
      this.apiKey = ((UsageBasedV1) license).key();
      this.appId = (String) config.originals().get(StreamsConfig.APPLICATION_ID_CONFIG);
      this.env = config.getString(ResponsiveConfig.RESPONSIVE_ENV_CONFIG);

      client = HttpClient.newHttpClient();
    }

    @Override
    public void accept(final Map<TopicPartition, Integer> counts, final String threadId) {
      LOG.debug("Reporting origin events for {} on {}", counts, threadId);

      final var body = OriginEventsReportRequestV1.builder()
          .setTimestamp(System.currentTimeMillis())
          .setTransactionId(UUID.randomUUID().toString())
          .setEventCount(counts.values().stream().reduce(Integer::sum).orElse(0))
          .setEnv(env)
          .setApplicationId(appId)
          .setThreadId(threadId)
          .build();

      try {
        final HttpRequest request = HttpRequest.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .uri(new URI(licenseServer + "/usage"))
            .header("Content-Type", "application/json")
            .header("Authorization", "Bearer " + apiKey)
            .POST(HttpRequest.BodyPublishers.ofString(mapper.writeValueAsString(body)))
            .build();

        client.sendAsync(request, HttpResponse.BodyHandlers.ofString())
            .thenAccept(response -> {
              final int status = response.statusCode();
              final String responseBody = response.body();
              if (status >= 400 && status < 500) {
                LOG.error(
                    "Failed to report usage due to {} (error code {}). "
                        + "Please make sure your license is correctly configured to "
                        + "avoid violation of the license agreement terms.",
                    responseBody,
                    status
                );
              } else if (status >= 500 && status < 600) {
                LOG.warn("Failed to report usage due to {} (code {}).", status, responseBody);
              } else {
                LOG.debug("Successfully reported usage (status: {}): {}", status, responseBody);
              }
            })
            .exceptionally(e -> {
              LOG.error("Failed to report usage! Please make sure "
                  + "you have correctly configured your Responsive License", e);
              return null;
            });
      } catch (final JsonProcessingException e) {
        // this should never happen
        LOG.warn("Internal error while reporting metrics to the License Server", e);
      } catch (final URISyntaxException e) {
        throw new IllegalArgumentException(
            "Invalid configuration for " + ResponsiveConfig.RESPONSIVE_LICENSE_SERVER_CONFIG + ": "
                + licenseServer, e
        );
      }
    }
  }
}
