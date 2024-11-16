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

package dev.responsive.kafka.internal.metrics;

@SuppressWarnings("checkstyle:linelength")
public class ApplicationMetrics {

  // Responsive application metrics scoped to the Streams client level
  public static final String APPLICATION_METRIC_GROUP = "application-metrics";

  public static final String STREAMS_STATE = "streams-state";
  public static final String STREAMS_STATE_DESCRIPTION = "The current KafkaStreams.State expressed as its ordinal value";

  public static final String NUM_RESTORING_CHANGELOGS = "num-restoring-changelogs";
  public static final String NUM_RESTORING_CHANGELOGS_DESCRIPTION = "The number of changelog topics currently being restored from";

  public static final String NUM_INTERRUPTED_CHANGELOGS = "num-interrupted-changelogs";
  public static final String NUM_INTERRUPTED_CHANGELOGS_DESCRIPTION = "The total number of changelog partitions that began restoring but did not complete";

  private ApplicationMetrics() {
  }
}
