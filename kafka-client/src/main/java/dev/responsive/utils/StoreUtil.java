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

import dev.responsive.kafka.api.InternalConfigs;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.TopologyDescription.Node;
import org.apache.kafka.streams.TopologyDescription.Source;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class StoreUtil {
  private static final Logger LOG = LoggerFactory.getLogger(StoreUtil.class);

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

  private StoreUtil() {
  }

  private static Optional<Boolean> isCompacted(final String topicName, final Admin admin) {
    final ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
    final List<ConfigResource> request = List.of(resource);
    final DescribeConfigsResult result = admin.describeConfigs(request);
    final Map<ConfigResource, Config> topicConfigs;
    try {
      topicConfigs = result.all().get();
    } catch (final InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
    if (!topicConfigs.containsKey(resource)) {
      return Optional.empty();
    }
    final Config topicConfig = topicConfigs.get(resource);
    final ConfigEntry cleanupPolicyEntry = topicConfig.get(TopicConfig.CLEANUP_POLICY_CONFIG);
    boolean compaction = cleanupPolicyEntry != null
        && cleanupPolicyEntry.value().contains(TopicConfig.CLEANUP_POLICY_COMPACT);
    return Optional.of(compaction);
  }

  private static boolean changelogIsSource(
      final TopologyDescription description,
      final String topicName
  ) {
    for (final var subtopology : description.subtopologies()) {
      for (final Node node : subtopology.nodes()) {
        if (node instanceof Source) {
          // We ignore the topic regex field here because tables can only source a single topic
          if (((Source) node).topicSet().contains(topicName)) {
            return true;
          }
        }
      }
    }
    return false;
  }

  public static boolean shouldTruncateChangelog(
      final String changelogTopic,
      final Admin admin,
      final Map<String, Object> appConfigs
  ) {
    // TODO: this is doing a lot of admin calls - we should cache the result
    final Optional<Boolean> compacted = isCompacted(changelogTopic, admin);
    final boolean source = changelogIsSource(
        InternalConfigs.loadTopologyDescription(appConfigs),
        changelogTopic
    );
    LOG.info("Changelog topic {}: compacted({}), source({})", changelogTopic, compacted, source);
    if (compacted.isEmpty()) {
      LOG.warn("Could not find topic {}. This should not happen. Will not truncate.",
          changelogTopic
      );
      return false;
    }
    if (compacted.get()) {
      LOG.info("Topic {} is compacted. Will not truncate.", changelogTopic);
      return false;
    }
    if (source) {
      LOG.info("Topic {} is a source topic. Will not truncate.", changelogTopic);
      return false;
    }
    return true;
  }
}
