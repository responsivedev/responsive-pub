/*
 * Copyright 2024 Responsive Computing, Inc.
 *
 * This source code is licensed under the Responsive Business Source License Agreement v1.0
 * available at:
 *
 * https://www.responsive.dev/legal/responsive-bsl-10
 *
 * This software requires a valid Commercial License Key for production use. Trial and commercial
 * licenses can be obtained at https://www.responsive.dev
 */

package dev.responsive.kafka.internal.metrics;

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.processor.TaskId;
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
  public static final String STORE_TAG = "store";
  public static final String TASK_ID_TAG = "task-id";
  public static final String PROCESSOR_NAME_TAG = "processor-name";

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
    final LinkedHashMap<String, String> applicationGroupTags = new LinkedHashMap<>();

    // IMPORTANT: DO NOT MODIFY THE ORDER OF INSERTION
    fillInApplicationTags(applicationGroupTags);
    fillInCustomUserTags(applicationGroupTags);

    return applicationGroupTags;
  }

  public LinkedHashMap<String, String> topicGroupTags(
      final String threadId,
      final TopicPartition topicPartition
  ) {
    final LinkedHashMap<String, String> topicGroupTags = new LinkedHashMap<>();

    // IMPORTANT: DO NOT MODIFY THE ORDER OF INSERTION
    fillInApplicationTags(topicGroupTags);
    fillInTopicTags(topicGroupTags, threadId, topicPartition);
    fillInCustomUserTags(topicGroupTags);

    return topicGroupTags;
  }

  public LinkedHashMap<String, String> threadGroupTags(final String threadId) {
    final LinkedHashMap<String, String> threadGroupTags = new LinkedHashMap<>();

    // IMPORTANT: DO NOT MODIFY THE ORDER OF INSERTION
    fillInApplicationTags(threadGroupTags);
    fillInThreadTags(threadGroupTags, threadId);
    fillInCustomUserTags(threadGroupTags);

    return threadGroupTags;
  }

  public LinkedHashMap<String, String> storeGroupTags(
      final String threadId,
      final TopicPartition topicPartition,
      final String storeName
  ) {
    final LinkedHashMap<String, String> storeGroupTags = new LinkedHashMap<>();

    // IMPORTANT: DO NOT MODIFY THE ORDER OF INSERTION
    fillInApplicationTags(storeGroupTags);
    fillInTopicTags(storeGroupTags, threadId, topicPartition);
    fillInStoreTags(storeGroupTags, storeName);
    fillInCustomUserTags(storeGroupTags);

    return storeGroupTags;
  }

  public LinkedHashMap<String, String> processorGroupTags(
      final String threadId,
      final TaskId taskId,
      final String processorName
  ) {
    final LinkedHashMap<String, String> processorGroupTags = new LinkedHashMap<>();

    // IMPORTANT: DO NOT MODIFY THE ORDER OF INSERTION
    fillInApplicationTags(processorGroupTags);
    fillInProcessorTags(processorGroupTags, threadId, taskId, processorName);
    fillInCustomUserTags(processorGroupTags);

    return processorGroupTags;
  }

  private void fillInApplicationTags(final LinkedHashMap<String, String> tags) {
    tags.put(RESPONSIVE_VERSION_TAG, responsiveClientVersion);
    tags.put(RESPONSIVE_COMMIT_ID_TAG, responsiveClientCommitId);
    tags.put(STREAMS_VERSION_TAG, streamsClientVersion);
    tags.put(STREAMS_COMMIT_ID_TAG, streamsClientCommitId);

    tags.put(CONSUMER_GROUP_TAG, consumerGroup);
    tags.put(STREAMS_APPLICATION_ID_TAG, streamsApplicationId);
    tags.put(STREAMS_CLIENT_ID_TAG, streamsClientId);
  }

  private void fillInThreadTags(
      final LinkedHashMap<String, String> tags,
      final String threadId
  ) {
    tags.put(THREAD_ID_TAG, threadId);
  }

  private void fillInTopicTags(
      final LinkedHashMap<String, String> tags,
      final String threadId,
      final TopicPartition topicPartition
  ) {
    tags.put(THREAD_ID_TAG, threadId);
    tags.put(TOPIC_TAG, topicPartition.topic());
    tags.put(PARTITION_TAG, Integer.toString(topicPartition.partition()));
  }

  private void fillInProcessorTags(
      final LinkedHashMap<String, String> tags,
      final String threadId,
      final TaskId taskId,
      final String processorName
  ) {
    tags.put(THREAD_ID_TAG, threadId);
    tags.put(TASK_ID_TAG, taskId.toString());
    tags.put(PROCESSOR_NAME_TAG, processorName);
  }

  private void fillInStoreTags(
      final LinkedHashMap<String, String> tags,
      final String storeName
  ) {
    tags.put(STORE_TAG, storeName);
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

