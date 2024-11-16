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

package dev.responsive.kafka.internal.utils;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;
import org.apache.kafka.clients.admin.Admin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class StoreUtil {

  private static final Logger LOG = LoggerFactory.getLogger(StoreUtil.class);

  // TODO: set up something like MetadataCache so we can cache the kafka partitions counts
  //  (It might even be possible to extract this from the assignor once we plug in one of our own)
  public static int numPartitionsForKafkaTopic(
      final Admin admin,
      final String topicName
  ) {
    try {
      return admin.describeTopics(List.of(topicName))
          .allTopicNames()
          .get(Constants.BLOCKING_TIMEOUT_VALUE, Constants.BLOCKING_TIMEOUT_UNIT)
          .get(topicName)
          .partitions()
          .size();
    } catch (final InterruptedException | ExecutionException | TimeoutException e) {
      throw new RuntimeException(e);
    }
  }

  public static long computeSegmentInterval(final long retentionPeriod, final long numSegments) {
    return Math.max(1, retentionPeriod / numSegments);
  }

  /**
   * Validate and convert the {@link Duration} to milliseconds
   */
  public static long durationToMillis(final Duration duration, final String parameterName) {
    final String errorMsgPrefix =
        String.format("Cannot convert %s Duration to milliseconds", parameterName);
    try {
      if (duration == null) {
        throw new IllegalArgumentException(errorMsgPrefix + " due to parameter being null");
      }

      return duration.toMillis();
    } catch (final ArithmeticException e) {
      throw new IllegalArgumentException(errorMsgPrefix + " due to arithmetic exception", e);
    }
  }

  public static String streamThreadId() {
    final String threadId = Thread.currentThread().getName();
    final var regex = Pattern.compile(".*-(StreamThread-\\d+)");
    final var match = regex.matcher(threadId);
    if (!match.find()) {
      LOG.warn("Unable to parse stream thread id from thread name = {}", threadId);
      return threadId;
    }
    return match.group(1);
  }

  private StoreUtil() {
  }
}
