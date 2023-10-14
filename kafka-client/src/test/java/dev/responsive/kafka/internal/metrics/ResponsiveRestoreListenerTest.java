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

import static dev.responsive.kafka.internal.metrics.ApplicationMetrics.NUM_INTERRUPTED_CHANGELOGS;
import static dev.responsive.kafka.internal.metrics.ApplicationMetrics.NUM_INTERRUPTED_CHANGELOGS_DESCRIPTION;
import static dev.responsive.kafka.internal.metrics.ApplicationMetrics.NUM_RESTORING_CHANGELOGS;
import static dev.responsive.kafka.internal.metrics.ApplicationMetrics.NUM_RESTORING_CHANGELOGS_DESCRIPTION;
import static dev.responsive.kafka.internal.metrics.StoreMetrics.TIME_RESTORING;
import static dev.responsive.kafka.internal.metrics.StoreMetrics.TIME_RESTORING_DESCRIPTION;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.CumulativeCount;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class ResponsiveRestoreListenerTest {

  private static final String THREAD_ID = Thread.currentThread().getName();
  private static final String APP_ID = "foo";
  private static final String CLIENT_ID = "foo-bar";

  private static final TopicPartition CHANGELOG1 = new TopicPartition("store-changelog", 1);
  private static final TopicPartition CHANGELOG2 = new TopicPartition("store-changelog", 2);
  private static final String STORE_NAME = "store";

  private static final Map<String, String> APPLICATION_TAGS = new LinkedHashMap<>();

  @Mock
  private Metrics metrics;
  @Mock
  private Sensor numInterruptedSensor;

  private ResponsiveMetrics responsiveMetrics;
  private ResponsiveRestoreListener restoreListener;

  @BeforeEach
  public void setup() {
    responsiveMetrics = new ResponsiveMetrics(metrics);
    responsiveMetrics.initializeTags(
        APP_ID,
        CLIENT_ID,
        new ClientVersionMetadata("1", "abc", "2", "dfe"),
        Collections.emptyMap()
    );

    APPLICATION_TAGS.put("responsive-version", "1");
    APPLICATION_TAGS.put("responsive-commit-id", "abc");
    APPLICATION_TAGS.put("streams-version", "2");
    APPLICATION_TAGS.put("streams-commit-id", "dfe");
    APPLICATION_TAGS.put("consumer-group", APP_ID);
    APPLICATION_TAGS.put("streams-application-id", APP_ID);
    APPLICATION_TAGS.put("streams-client-id", CLIENT_ID);

    Mockito.when(metrics.sensor(NUM_INTERRUPTED_CHANGELOGS)).thenReturn(numInterruptedSensor);
    restoreListener = new ResponsiveRestoreListener(responsiveMetrics);
    Mockito.verify(numInterruptedSensor).add(
        eq(new MetricName(
            NUM_INTERRUPTED_CHANGELOGS,
            "application-metrics",
            NUM_INTERRUPTED_CHANGELOGS_DESCRIPTION,
            APPLICATION_TAGS)),
        any(CumulativeCount.class));
  }

  private MetricName numRestoringChangelogsMetricName() {
    return new MetricName(
        NUM_RESTORING_CHANGELOGS,
        "application-metrics",
        NUM_RESTORING_CHANGELOGS_DESCRIPTION,
        APPLICATION_TAGS
    );
  }

  private MetricName timeRestoringMetricName(final TopicPartition changelog) {
    return new MetricName(
        TIME_RESTORING,
        "store-metrics",
        TIME_RESTORING_DESCRIPTION,
        storeLevelTags(changelog)
    );
  }

  @Test
  public void shouldAddAndRemoveStoreMetrics() {
    Mockito.verify(metrics).addMetric(eq(numRestoringChangelogsMetricName()), any(Gauge.class));

    restoreListener.onRestoreStart(CHANGELOG1, STORE_NAME, 0L, 100L);
    restoreListener.onRestoreStart(CHANGELOG2, STORE_NAME, 0L, 100L);
    Mockito.verify(metrics).addMetric(eq(timeRestoringMetricName(CHANGELOG1)), any(Gauge.class));
    Mockito.verify(metrics).addMetric(eq(timeRestoringMetricName(CHANGELOG2)), any(Gauge.class));

    restoreListener.onBatchRestored(CHANGELOG1, STORE_NAME, 50L, 50L);
    restoreListener.onBatchRestored(CHANGELOG1, STORE_NAME, 50L, 100L);
    restoreListener.onBatchRestored(CHANGELOG2, STORE_NAME, 50L, 50L);

    restoreListener.onRestoreEnd(CHANGELOG1, STORE_NAME, 100L);
    restoreListener.onRestoreSuspended(CHANGELOG2, STORE_NAME, 50L);

    Mockito.verify(metrics).removeMetric(timeRestoringMetricName(CHANGELOG1));
    Mockito.verify(metrics).removeMetric(timeRestoringMetricName(CHANGELOG2));
    Mockito.verify(numInterruptedSensor).record(eq(1.0d), any(Long.class));

    restoreListener.close();
    Mockito.verify(metrics).removeMetric(numRestoringChangelogsMetricName());
    Mockito.verify(metrics).removeSensor(NUM_INTERRUPTED_CHANGELOGS);
  }

  @Test
  public void shouldDelegateToUserRestoreListener() {
    final UserRestoreListener userListener = new UserRestoreListener();
    restoreListener.registerUserRestoreListener(userListener);

    restoreListener.onRestoreStart(CHANGELOG1, STORE_NAME, 0L, 100L);
    restoreListener.onRestoreStart(CHANGELOG2, STORE_NAME, 0L, 100L);

    restoreListener.onBatchRestored(CHANGELOG1, STORE_NAME, 50L, 50L);
    restoreListener.onBatchRestored(CHANGELOG1, STORE_NAME, 50L, 100L);
    restoreListener.onBatchRestored(CHANGELOG2, STORE_NAME, 50L, 50L);

    restoreListener.onRestoreEnd(CHANGELOG1, STORE_NAME, 100L);
    restoreListener.onRestoreSuspended(CHANGELOG2, STORE_NAME, 50L);

    restoreListener.close();

    assertThat(userListener.onRestoreStartCalls, equalTo(2));
    assertThat(userListener.onBatchRestoredCalls, equalTo(3));
    assertThat(userListener.onRestoreEndCalls, equalTo(1));
    // TODO(3.5+): uncomment this out once we upgrade to 3.5 or above
    //assertThat(userListener.onRestoreSuspendedCalls, equalTo(1));
  }

  private Map<String, String> storeLevelTags(final TopicPartition topicPartition) {
    final Map<String, String> tags = new HashMap<>(APPLICATION_TAGS);
    tags.put("thread-id", THREAD_ID);
    tags.put("topic", topicPartition.topic());
    tags.put("partition", Integer.toString(topicPartition.partition()));
    tags.put("store", STORE_NAME);
    return tags;
  }

  private static class UserRestoreListener implements StateRestoreListener {
    int onRestoreStartCalls = 0;
    int onBatchRestoredCalls = 0;
    int onRestoreEndCalls = 0;
    int onRestoreSuspendedCalls = 0;

    @Override
    public void onRestoreStart(
        final TopicPartition topicPartition,
        final String storeName,
        final long startingOffset,
        final long endingOffset
    ) {
      ++onRestoreStartCalls;
    }

    @Override
    public void onBatchRestored(
        final TopicPartition topicPartition,
        final String storeName,
        final long batchEndOffset,
        final long numRestored
    ) {
      ++onBatchRestoredCalls;
    }

    @Override
    public void onRestoreEnd(
        final TopicPartition topicPartition,
        final String storeName,
        final long totalRestored
    ) {
      ++onRestoreEndCalls;
    }

    // TODO(3.5+): uncomment out the override annotation once we upgrade to 3.5 or above
    //@Override
    public void onRestoreSuspended(
        final TopicPartition topicPartition,
        final String storeName,
        final long totalRestored
    ) {
      ++onRestoreSuspendedCalls;
    }
  }
}
