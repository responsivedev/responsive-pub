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

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple wrapper around the tag map, so we can control the insertion order
 * and make sure the tag ordering stays consistent with the mbean name
 */
public class OrderedTagsSupplier {

  private static final Logger LOG = LoggerFactory.getLogger(OrderedTagsSupplier.class);

  // Base tags that all Responsive metrics are tagged with:
  public static final String RESPONSIVE_VERSION_TAG = "responsive-version";
  public static final String RESPONSIVE_COMMIT_ID_TAG = "responsive-commit-id";
  public static final String STREAMS_VERSION_TAG = "streams-version";
  public static final String STREAMS_COMMIT_ID_TAG = "streams-commit-id";
  public static final String CONSUMER_GROUP_TAG = "consumer-group";
  public static final String STREAMS_APPLICATION_ID_TAG = "streams-application-id";
  public static final String STREAMS_CLIENT_ID_TAG = "streams-client-id";

  // Group tags that are specific to the given metric group and scope
  public static final String THREAD_ID_TAG = "thread-id";
  public static final String TOPIC_TAG = "topic";
  public static final String PARTITION_TAG = "partition";
  public static final String STORE_NAME_TAG = "store-name";

  private final String responsiveClientVersion;
  private final String responsiveClientCommitId;
  private final String streamsClientVersion;
  private final String streamsClientCommitId;
  private final String consumerGroup;
  private final String streamsApplicationId;
  private final String streamsClientId;
  private final List<Entry<String, String>> orderedUserTags;

  @SuppressWarnings("unchecked")
  public OrderedTagsSupplier(
      final String responsiveClientVersion,
      final String responsiveClientCommitId,
      final String streamsClientVersion,
      final String streamsClientCommitId,
      final String consumerGroup,
      final String streamsApplicationId,
      final String streamsClientId,
      final Map<String, ?> userTags
  ) {
    this.responsiveClientVersion = responsiveClientVersion;
    this.responsiveClientCommitId = responsiveClientCommitId;
    this.streamsClientVersion = streamsClientVersion;
    this.streamsClientCommitId = streamsClientCommitId;
    this.consumerGroup = consumerGroup;
    this.streamsApplicationId = streamsApplicationId;
    this.streamsClientId = streamsClientId;

    this.orderedUserTags = userTags.entrySet()
        .stream()
        .map(e -> (Map.Entry<String, String>) e)
        .sorted(Entry.<String, String>comparingByKey()
                    .thenComparing(Entry.comparingByValue(Comparator.comparing(v -> v))))
        .collect(Collectors.toList());
  }

  public LinkedHashMap<String, String> applicationGroupTags() {
    // IMPORTANT: DO NOT MODIFY THE ORDER OF INSERTION
    final LinkedHashMap<String, String> applicationGroupTags = new LinkedHashMap<>();

    applicationGroupTags.put(RESPONSIVE_VERSION_TAG, responsiveClientVersion);
    applicationGroupTags.put(RESPONSIVE_COMMIT_ID_TAG, responsiveClientCommitId);
    applicationGroupTags.put(STREAMS_VERSION_TAG, streamsClientVersion);
    applicationGroupTags.put(STREAMS_COMMIT_ID_TAG, streamsClientCommitId);

    applicationGroupTags.put(CONSUMER_GROUP_TAG, consumerGroup);
    applicationGroupTags.put(STREAMS_APPLICATION_ID_TAG, streamsApplicationId);
    applicationGroupTags.put(STREAMS_CLIENT_ID_TAG, streamsClientId);

    fillInCustomUserTags(applicationGroupTags);
    return applicationGroupTags;
  }

  public LinkedHashMap<String, String> topicGroupTags(
      final String threadId,
      final TopicPartition topicPartition
  ) {
    // IMPORTANT: DO NOT MODIFY THE ORDER OF INSERTION
    final LinkedHashMap<String, String> topicGroupTags = applicationGroupTags();
    topicGroupTags.put(THREAD_ID_TAG, threadId);
    topicGroupTags.put(TOPIC_TAG, topicPartition.topic());
    topicGroupTags.put(PARTITION_TAG, Integer.toString(topicPartition.partition()));

    fillInCustomUserTags(topicGroupTags);
    return topicGroupTags;
  }

  public LinkedHashMap<String, String> storeGroupTags(
      final String threadId,
      final TopicPartition topicPartition,
      final String storeName
  ) {
    // IMPORTANT: DO NOT MODIFY THE ORDER OF INSERTION
    final LinkedHashMap<String, String> storeGroupTags = topicGroupTags(threadId, topicPartition);
    storeGroupTags.put(STORE_NAME_TAG, storeName);

    fillInCustomUserTags(storeGroupTags);
    return storeGroupTags;
  }

  // Fill these in last, after all the Responsive tags
  private void fillInCustomUserTags(final LinkedHashMap<String, String> tags) {
    for (final var tag : orderedUserTags) {
      final String tagKey = tag.getKey();
      final String tagValue = tag.getValue();
      LOG.debug("Adding custom metric tag <{}:{}>", tagKey, tagValue);
      tags.put(tagKey, tagValue);
    }
  }

}

