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

package dev.responsive.utils;

import java.time.Duration;
import java.util.Map;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class StoreUtil {

  private static final Logger LOG = LoggerFactory.getLogger(StoreUtil.class);

  public static void validateTopologyOptimizationConfig(final StreamsConfig config) {
    final String optimizations = config.getString(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG);
    if (optimizations.equals(StreamsConfig.OPTIMIZE)
        || optimizations.contains(StreamsConfig.REUSE_KTABLE_SOURCE_TOPICS)) {
      LOG.error("Responsive stores are not currently compatible with the source topic optimization."
                    + " This application was configured with {}", optimizations);
      throw new IllegalArgumentException(
          "Responsive stores cannot be used with reuse.ktable.source.topics optimization, please "
              + "reach out to us if you are attempting to migrate an existing application that "
              + "uses this optimization. For new applications, please disable this optimization "
              + "by setting only the desired subset of optimizations, or else disabling all of"
              + "them. See StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIGS for the list of options");
    }
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
