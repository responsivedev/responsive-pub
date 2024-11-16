/*
 * Copyright 2024 Responsive Computing, Inc.
 *
 * This source code is licensed under the Responsive Software License Agreement v1.0 available at
 *
 * https://www.responsive.dev/legal/responsive-bsl-10
 *
 * This software requires a valid Commercial License Key for production use. Trial and commercial
 * licenses can be obtained at https://www.responsive.dev/sdk/get-started.
 */

package dev.responsive.kafka.internal.db;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.retry.RetryDecision;
import com.datastax.oss.driver.api.core.retry.RetryVerdict;
import com.datastax.oss.driver.api.core.servererrors.WriteType;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.internal.core.retry.DefaultRetryPolicy;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.CumulativeCount;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResponsiveRetryPolicy extends DefaultRetryPolicy {

  private static final Logger LOG = LoggerFactory.getLogger(ResponsiveRetryPolicy.class);

  private static final String METRICS_GROUP = "responsive.cassandra.retry";

  private final String logPrefix;
  private final Metrics metrics;
  private final Sensor writeTimeouts;

  public ResponsiveRetryPolicy(
      final DriverContext context,
      final String profileName
  ) {
    super(context, profileName);
    this.logPrefix = (context != null ? context.getSessionName() : null) + "|" + profileName;
    this.metrics = new Metrics(
        new MetricConfig(),
        List.of(new JmxReporter()),
        Time.SYSTEM,
        new KafkaMetricsContext("dev.responsive", new HashMap<>())
    );
    this.writeTimeouts = registerWriteTimeoutSensor(metrics, context, profileName);
  }

  private Sensor registerWriteTimeoutSensor(
      final Metrics metrics,
      final DriverContext context,
      final String profile
  ) {
    final var sensor = metrics.sensor("write-timeouts-total");
    sensor.add(
        new MetricName(
            "write-timeouts-total",
            METRICS_GROUP,
            "total write timeouts",
            Map.of(
                "profile", profile,
                "session", context == null ? "" : context.getSessionName()
            )
        ),
        new CumulativeCount()
    );
    return sensor;
  }

  @Override
  public RetryVerdict onWriteTimeoutVerdict(
      @NonNull final Request request,
      @NonNull final ConsistencyLevel cl,
      @NonNull final WriteType writeType,
      final int blockFor,
      final int received,
      final int retryCount
  ) {
    return () -> {
      writeTimeouts.record();

      // this differs from the default policy in that we know all writes to C*
      // are retry-able, not just BATCH LOG writes because all BATCH UNLOGGED
      // writes that our client issues go to only a single partition - we also
      // up the retry rate from 1 retry to 3 retries
      RetryDecision decision = (retryCount < 3)
          ? RetryDecision.RETRY_SAME
          : RetryDecision.RETHROW;

      if (decision == RetryDecision.RETRY_SAME && LOG.isTraceEnabled()) {
        LOG.trace(
            RETRYING_ON_WRITE_TIMEOUT, logPrefix, cl, writeType, blockFor, received, retryCount);
      } else if (decision.equals(RetryDecision.RETHROW)) {
        final var msg = "[{}] Rethrowing write timeout error due to too many retries "
            + "(consistency: {}, write type: {}, required acknowledgments: {}, "
            + "received acknowledgments: {}, retries: {})";
        LOG.error(msg, logPrefix, cl, writeType, blockFor, received, retryCount);
      }

      return decision;
    };
  }

  @Override
  public void close() {
    metrics.close();
  }
}