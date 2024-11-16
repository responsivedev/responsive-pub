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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

public class OrderedTagsSupplierTest {

  private static final String RESPONSIVE_VERSION = "0.666.0";
  private static final String RESPONSIVE_COMMIT = "abcde";
  private static final String STREAMS_VERSION = "1.2.3";
  private static final String STREAMS_COMMIT = "zyxwv";
  private static final String APPLICATION_ID = "ordered-tags-test";
  private static final String CLIENT_ID = "ordered-tags-test-node-1";

  private static final String THREAD = "StreamThread-1";
  private static final TopicPartition TOPIC_PARTITION = new TopicPartition("input", 1);

  private static final String STORE = "count-store";

  private static final Map<String, ?> userTags = Map.of(
      "custom-tag-1", "C",
      "custom-tag-2", "B",
      "custom-tag-3", "A"
  );

  private final OrderedTagsSupplier tagSupplier = new OrderedTagsSupplier(
      RESPONSIVE_VERSION,
      RESPONSIVE_COMMIT,
      STREAMS_VERSION,
      STREAMS_COMMIT,
      APPLICATION_ID,
      APPLICATION_ID,
      CLIENT_ID,
      userTags
  );

  @Test
  public void shouldReturnApplicationGroupTagsInOrder() {
    final Map<String, String> tags = tagSupplier.applicationGroupTags();
    final Iterator<Entry<String, String>> tagsIter = tags.entrySet().iterator();

    verifyBaseTags(tagsIter);
    verifyUserTags(tagsIter);

    assertFalse(tagsIter.hasNext());
  }

  @Test
  public void shouldReturnTopicGroupTagsInOrder() {
    final Map<String, String> tags = tagSupplier.topicGroupTags(THREAD, TOPIC_PARTITION);
    final Iterator<Entry<String, String>> tagsIter = tags.entrySet().iterator();

    verifyBaseTags(tagsIter);
    verifyTopicTags(tagsIter);
    verifyUserTags(tagsIter);

    assertFalse(tagsIter.hasNext());
  }

  @Test
  public void shouldReturnStoreGroupTagsInOrder() {
    final Map<String, String> tags = tagSupplier.storeGroupTags(THREAD, TOPIC_PARTITION, STORE);
    final Iterator<Entry<String, String>> tagsIter = tags.entrySet().iterator();

    verifyBaseTags(tagsIter);
    verifyTopicTags(tagsIter);
    verifyStoreTags(tagsIter);
    verifyUserTags(tagsIter);

    assertFalse(tagsIter.hasNext());
  }

  private void verifyBaseTags(final Iterator<Entry<String, String>> tagsIter) {
    Map.Entry<String, String> tag = tagsIter.next();
    assertThat(tag.getKey(), equalTo("responsive-version"));
    assertThat(tag.getValue(), equalTo("0.666.0"));

    tag = tagsIter.next();
    assertThat(tag.getKey(), equalTo("responsive-commit-id"));
    assertThat(tag.getValue(), equalTo("abcde"));

    tag = tagsIter.next();
    assertThat(tag.getKey(), equalTo("streams-version"));
    assertThat(tag.getValue(), equalTo("1.2.3"));

    tag = tagsIter.next();
    assertThat(tag.getKey(), equalTo("streams-commit-id"));
    assertThat(tag.getValue(), equalTo("zyxwv"));

    tag = tagsIter.next();
    assertThat(tag.getKey(), equalTo("consumer-group"));
    assertThat(tag.getValue(), equalTo("ordered-tags-test"));

    tag = tagsIter.next();
    assertThat(tag.getKey(), equalTo("streams-application-id"));
    assertThat(tag.getValue(), equalTo("ordered-tags-test"));

    tag = tagsIter.next();
    assertThat(tag.getKey(), equalTo("streams-client-id"));
    assertThat(tag.getValue(), equalTo("ordered-tags-test-node-1"));
  }

  private void verifyTopicTags(final Iterator<Entry<String, String>> tagsIter) {
    Map.Entry<String, String> tag = tagsIter.next();
    assertThat(tag.getKey(), equalTo("thread-id"));
    assertThat(tag.getValue(), equalTo("StreamThread-1"));

    tag = tagsIter.next();
    assertThat(tag.getKey(), equalTo("topic"));
    assertThat(tag.getValue(), equalTo("input"));

    tag = tagsIter.next();
    assertThat(tag.getKey(), equalTo("partition"));
    assertThat(tag.getValue(), equalTo("1"));
  }

  private void verifyStoreTags(final Iterator<Entry<String, String>> tagsIter) {
    Map.Entry<String, String> tag = tagsIter.next();
    assertThat(tag.getKey(), equalTo("store"));
    assertThat(tag.getValue(), equalTo("count-store"));
  }

  private void verifyUserTags(final Iterator<Entry<String, String>> tagsIter) {
    Map.Entry<String, String> tag = tagsIter.next();
    assertThat(tag.getKey(), equalTo("custom-tag-1"));
    assertThat(tag.getValue(), equalTo("C"));

    tag = tagsIter.next();
    assertThat(tag.getKey(), equalTo("custom-tag-2"));
    assertThat(tag.getValue(), equalTo("B"));

    tag = tagsIter.next();
    assertThat(tag.getKey(), equalTo("custom-tag-3"));
    assertThat(tag.getValue(), equalTo("A"));
  }

}
