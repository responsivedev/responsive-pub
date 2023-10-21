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
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.config.TopicConfig;
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
          .get()
          .get(topicName)
          .partitions()
          .size();
    } catch (final InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  public static long computeSegmentInterval(final long retentionPeriod, final int numSegments) {
    return retentionPeriod / numSegments;
  }

  public static void validateLogConfigs(
      final Map<String, String> config,
      final boolean truncateChangelog,
      final String storeName
  ) {
    if (truncateChangelog) {
      final String cleanupPolicy = config.get(TopicConfig.CLEANUP_POLICY_CONFIG);
      if (cleanupPolicy != null && !cleanupPolicy.contains(TopicConfig.CLEANUP_POLICY_DELETE)) {
        LOG.error("Store {} is configured with changelog truncation enabled, which requires "
                      + "the cleanup policy to include 'delete'. Please set {} to either "
                      + "[{}] or [{}, {}]",
                  storeName, TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE,
                  TopicConfig.CLEANUP_POLICY_DELETE, TopicConfig.CLEANUP_POLICY_COMPACT);
        throw new IllegalArgumentException(String.format(
            "Truncated changelogs must set %s to either [%s] or [%s, %s]. Got [%s] for store %s.",
            TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE,
            TopicConfig.CLEANUP_POLICY_DELETE, TopicConfig.CLEANUP_POLICY_COMPACT,
            cleanupPolicy, storeName)
        );
      }
    }
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
