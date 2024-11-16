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
public class TopicMetrics {
  
  public static final String TOPIC_METRIC_GROUP = "topic-metrics";

  public static final String COMMITTED_OFFSET = "committed-offset";
  public static final String COMMITTED_OFFSET_DESCRIPTION = "The latest committed offset of this topic partition";

  public static final String END_OFFSET = "end-offset";
  public static final String END_OFFSET_DESCRIPTION = "The end offset of this topic partition";
}
