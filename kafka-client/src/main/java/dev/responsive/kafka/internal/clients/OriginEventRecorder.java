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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OriginEventRecorder implements ResponsiveConsumer.Listener {

  private static final Logger LOG = LoggerFactory.getLogger(OriginEventRecorder.class);

  static final String ORIGIN_EVENT_HEADER_KEY = "_roe";
  static final byte[] ORIGIN_EVENT_MARK = new byte[] {0x1};

  private final Duration reportInterval;
  private final BiConsumer<Map<TopicPartition, Long>, String> reportOriginEvents;
  private final String threadId;

  @GuardedBy("this")
  private final Map<TopicPartition, Long> originEvents = new HashMap<>();
  @GuardedBy("this")
  private long lastCommitTs = 0L;

  public OriginEventRecorder(
      final String threadId,
      final ResponsiveConfig config,
      LicenseInfo license
  ) {
    this(
        threadId,
        new Reporter(config, license),
        config.getLong(ResponsiveConfig.ORIGIN_EVENT_REPORT_INTERVAL_MS_CONFIG)
    );
  }

  @VisibleForTesting
  OriginEventRecorder(
      final String threadId,
      final BiConsumer<Map<TopicPartition, Long>, String> reportOriginEvents
  ) {
    this(threadId, reportOriginEvents, 10_000);
  }

  private OriginEventRecorder(
      final String threadId,
      final BiConsumer<Map<TopicPartition, Long>, String> reportOriginEvents,
      final long reportIntervalMs
  ) {
    this.threadId = threadId;
    this.reportOriginEvents = reportOriginEvents;
    this.reportInterval = Duration.ofMillis(reportIntervalMs);
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
    final var now = System.currentTimeMillis();
    final var timeSinceReport = Duration.ofMillis(now - lastCommitTs);

    if (timeSinceReport.compareTo(reportInterval) > 0) {
      final var report = new HashMap<TopicPartition, Long>();

      synchronized (this) {
        lastCommitTs = now;
        for (var tp : offsets.keySet()) {
          final var originCount = originEvents.get(tp);
          if (originCount != null) {
            report.put(tp, originCount);
          }
          originEvents.put(tp, 0L);
        }
      }

      reportOriginEvents.accept(report, threadId);
    }
  }

  @Override
  public synchronized void onPartitionsLost(final Collection<TopicPartition> partitions) {
    partitions.forEach(originEvents::remove);
  }

  @Override
  public synchronized void onUnsubscribe() {
    originEvents.clear();
  }

  private static class Reporter implements BiConsumer<Map<TopicPartition, Long>, String> {

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
    public void accept(final Map<TopicPartition, Long> counts, final String threadId) {
      LOG.debug("Reporting origin events for {} on {}", counts, threadId);

      final var body = OriginEventsReportRequestV1.builder()
          .setTimestamp(System.currentTimeMillis())
          .setTransactionId(UUID.randomUUID().toString())
          .setEventCount(counts.values().stream().reduce(Long::sum).orElse(0L))
          .setEnv(env)
          .setApplicationId(appId)
          .setThreadId(threadId)
          .build();

      try {
        final HttpRequest request = HttpRequest.newBuilder()
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
      } catch (final JsonProcessingException | URISyntaxException e) {
        // this should never happen
        throw new RuntimeException(e);
      }
    }
  }
}
