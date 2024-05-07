/*
 * Copyright 2023 Responsive Computing, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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

import java.util.LinkedHashMap;

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

  public static final String FAILED_TRUNCATIONS = "failed-truncations";
  public static final String FAILED_TRUNCATIONS_DESCRIPTION = "changelog truncation attempts that failed";
  public static final String FAILED_TRUNCATIONS_RATE = FAILED_TRUNCATIONS + RATE_SUFFIX;
  public static final String FAILED_TRUNCATIONS_RATE_DESCRIPTION = RATE_DESCRIPTION + FAILED_TRUNCATIONS_DESCRIPTION;
  public static final String FAILED_TRUNCATIONS_TOTAL = FAILED_TRUNCATIONS + TOTAL_SUFFIX;
  public static final String FAILED_TRUNCATIONS_TOTAL_DESCRIPTION = TOTAL_DESCRIPTION + FAILED_TRUNCATIONS_DESCRIPTION;
}
