/*
 * Copyright 2025 Responsive Computing, Inc.
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
import com.google.common.util.concurrent.AbstractScheduledService;
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
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OriginEventReporterImpl extends AbstractScheduledService
    implements OriginEventReporter {

  private static final Logger LOG = LoggerFactory.getLogger(OriginEventReporterImpl.class);

  private final ObjectMapper mapper = new ObjectMapper();
  private final HttpClient client;
  private final String licenseServer;
  private final String apiKey;
  private final String appId;
  private final String env;
  private final long reportInterval;

  @GuardedBy("this")
  private final Map<TopicPartition, Integer> currentReport;
  private CompletableFuture<Void> inFlight;

  public OriginEventReporterImpl(ResponsiveConfig config, final LicenseInfo license) {
    if (!license.type().equals(UsageBasedV1.TYPE_NAME)) {
      throw new IllegalArgumentException(
          "Invalid license type for usage reporting: " + license.type());
    }

    this.licenseServer = config.getString(ResponsiveConfig.RESPONSIVE_LICENSE_SERVER_CONFIG);
    this.apiKey = ((UsageBasedV1) license).key();
    this.appId = (String) config.originals().get(StreamsConfig.APPLICATION_ID_CONFIG);
    this.env = config.getString(ResponsiveConfig.RESPONSIVE_ENV_CONFIG);
    this.reportInterval = config.getLong(ResponsiveConfig.ORIGIN_EVENT_REPORT_INTERVAL_MS_CONFIG);

    client = HttpClient.newHttpClient();
    currentReport = new HashMap<>();
  }

  @Override
  public synchronized void report(final TopicPartition tp, Integer count) {
    currentReport.compute(tp, (k, old) -> old == null ? count : old + count);
  }

  @Override
  protected void runOneIteration() {
    final Map<TopicPartition, Integer> report;
    synchronized (this) {
      // don't bother reporting an empty report
      if (currentReport.isEmpty()) {
        return;
      }

      report = Map.copyOf(currentReport);
      currentReport.clear();
    }

    LOG.info("Reporting origin events for {}", report);

    final var body = OriginEventsReportRequestV1.builder()
        .setTimestamp(System.currentTimeMillis())
        .setTransactionId(UUID.randomUUID().toString())
        .setEventCount(report.values().stream().reduce(Integer::sum).orElse(0))
        .setEnv(env)
        .setApplicationId(appId)
        .build();

    try {
      final HttpRequest request = HttpRequest.newBuilder()
          .version(HttpClient.Version.HTTP_1_1)
          .uri(new URI(licenseServer + "/v1/usage"))
          .header("Content-Type", "application/json")
          .header("Authorization", "Bearer " + apiKey)
          .POST(HttpRequest.BodyPublishers.ofString(mapper.writeValueAsString(body)))
          .build();

      this.inFlight = client.sendAsync(request, HttpResponse.BodyHandlers.ofString())
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

  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedRateSchedule(
        Duration.ofMillis(reportInterval),
        Duration.ofMillis(reportInterval)
    );
  }

  @Override
  public void start() {
    startAsync().awaitRunning();
  }

  @Override
  public void close() {
    // if there's any data that has not been reported, report it now
    // and await the report -- this also helps ensure end-to-end testing
    // is accurate
    runOneIteration();
    if (inFlight != null) {
      try {
        // if server doesn't respond in time just drop the usage
        // rather the hold up shutdown; this is only really important
        // for testing situations
        inFlight.get(10, TimeUnit.SECONDS);
      } catch (final Exception e) {
        LOG.warn("Could not await final origin event report.", e);
      }
    }
    stopAsync().awaitTerminated();
  }
}
