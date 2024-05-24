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

package dev.responsive.kafka.internal.utils;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
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

  private StoreUtil() {
  }
}
