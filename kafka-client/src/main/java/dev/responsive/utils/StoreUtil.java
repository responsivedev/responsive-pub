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

public final class StoreUtil {
  public static void validateTopologyOptimizationConfig(final Map<String, Object> configs) {
    final String optimizations = new StreamsConfig(configs)
        .getString(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG);
    if (optimizations.equals(StreamsConfig.OPTIMIZE)
        || optimizations.contains(StreamsConfig.REUSE_KTABLE_SOURCE_TOPICS)) {
      throw new IllegalArgumentException(
          "Responsive stores cannot be used with reuse.ktable.source.topics optimization");
    }
  }

  public static void validateLogConfigs(
      final Map<String, String> config,
      final boolean truncateChangelog
  ) {
    if (truncateChangelog) {
      final String cleanupPolicy = config.get(TopicConfig.CLEANUP_POLICY_CONFIG);
      if (cleanupPolicy != null && !cleanupPolicy.equals(TopicConfig.CLEANUP_POLICY_DELETE)) {
        throw new IllegalArgumentException(String.format(
            "Changelogs must use %s=[%s]. Got [%s]",
            TopicConfig.CLEANUP_POLICY_CONFIG,
            TopicConfig.CLEANUP_POLICY_DELETE,
            cleanupPolicy)
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
}
