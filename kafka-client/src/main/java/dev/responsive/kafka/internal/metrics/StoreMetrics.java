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

package dev.responsive.kafka.internal.metrics;

import static dev.responsive.kafka.internal.metrics.ResponsiveMetrics.AVG_DESCRIPTION;
import static dev.responsive.kafka.internal.metrics.ResponsiveMetrics.AVG_SUFFIX;
import static dev.responsive.kafka.internal.metrics.ResponsiveMetrics.MAX_DESCRIPTION;
import static dev.responsive.kafka.internal.metrics.ResponsiveMetrics.MAX_SUFFIX;
import static dev.responsive.kafka.internal.metrics.ResponsiveMetrics.RATE_DESCRIPTION;
import static dev.responsive.kafka.internal.metrics.ResponsiveMetrics.RATE_SUFFIX;
import static dev.responsive.kafka.internal.metrics.ResponsiveMetrics.TOTAL_DESCRIPTION;
import static dev.responsive.kafka.internal.metrics.ResponsiveMetrics.TOTAL_SUFFIX;

@SuppressWarnings("checkstyle:linelength")
public class StoreMetrics {
  // Responsive store metrics scoped to the individual state store level
  public static final String STORE_METRIC_GROUP = "store-metrics";

  public static final String TIME_RESTORING = "time-restoring";
  public static final String TIME_RESTORING_DESCRIPTION = "The amount of time (in ms) since this state store started restoration";

  public static final String TIME_SINCE_LAST_FLUSH = "time-since-last-flush";
  public static final String TIME_SINCE_LAST_FLUSH_DESCRIPTION = "The amount of time (in ms) since the last successful flush";

  public static final String FLUSH = "flush";
  public static final String FLUSH_DESCRIPTION = "flushes of the commit buffer";
  public static final String FLUSH_RATE = FLUSH + RATE_SUFFIX;
  public static final String FLUSH_RATE_DESCRIPTION = RATE_DESCRIPTION + FLUSH_DESCRIPTION;
  public static final String FLUSH_TOTAL = FLUSH + TOTAL_SUFFIX;
  public static final String FLUSH_TOTAL_DESCRIPTION = TOTAL_DESCRIPTION + FLUSH_DESCRIPTION;

  public static final String FLUSH_LATENCY = "flush-latency";
  public static final String FLUSH_LATENCY_DESCRIPTION = "amount of time (in ms) it took to flush the commit buffer";
  public static final String FLUSH_LATENCY_AVG = FLUSH_LATENCY + AVG_SUFFIX;
  public static final String FLUSH_LATENCY_AVG_DESCRIPTION = AVG_DESCRIPTION + FLUSH_LATENCY_DESCRIPTION;
  public static final String FLUSH_LATENCY_MAX = FLUSH_LATENCY + MAX_SUFFIX;
  public static final String FLUSH_LATENCY_MAX_DESCRIPTION = MAX_DESCRIPTION + FLUSH_LATENCY_DESCRIPTION;

  public static final String FLUSH_ERRORS = "flush-errors";
  public static final String FLUSH_ERRORS_DESCRIPTION = "buffer flush attempts during a commit that failed";
  public static final String FLUSH_ERRORS_RATE = FLUSH_ERRORS + RATE_SUFFIX;
  public static final String FLUSH_ERRORS_RATE_DESCRIPTION = RATE_DESCRIPTION + FLUSH_ERRORS_DESCRIPTION;
  public static final String FLUSH_ERRORS_TOTAL = FLUSH_ERRORS + TOTAL_SUFFIX;
  public static final String FLUSH_ERRORS_TOTAL_DESCRIPTION = TOTAL_DESCRIPTION + FLUSH_ERRORS_DESCRIPTION;
}
