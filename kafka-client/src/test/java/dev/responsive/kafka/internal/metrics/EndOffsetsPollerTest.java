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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import dev.responsive.kafka.internal.metrics.EndOffsetsPoller;
import dev.responsive.kafka.internal.metrics.EndOffsetsPoller.Factories;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.MetricValueProvider;
import org.apache.kafka.common.metrics.Metrics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class EndOffsetsPollerTest {
  private static final Map<String, Object> CONFIGS = Map.of();
  private static final String THREAD_ID = "StreamThread-0";
  private static final TopicPartition PARTITION1 = new TopicPartition("alice", 1);
  private static final TopicPartition PARTITION2 = new TopicPartition("bob", 2);

  @Mock
  private AdminClient adminClient;
  @Mock
  private Factories factories;
  @Mock
  private Metrics metrics;
  @Mock
  private ScheduledExecutorService executor;
  @Mock
  private ScheduledFuture<Object> pollFuture;
  @Captor
  private ArgumentCaptor<MetricName> metricNameCaptor;
  @Captor
  private ArgumentCaptor<MetricValueProvider<Long>> valueProviderCaptor;
  @Captor
  private ArgumentCaptor<Runnable> taskCaptor;
  private EndOffsetsPoller endOffsetsPoller;

  @BeforeEach
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void setup() {
    lenient().when(factories.createAdminClient(anyMap())).thenReturn(adminClient);
    lenient().when(executor.scheduleAtFixedRate(any(), anyLong(), anyLong(), any()))
        .thenReturn((ScheduledFuture) pollFuture);
    endOffsetsPoller = new EndOffsetsPoller(CONFIGS, metrics, executor, factories);
  }

  @Test
  public void shouldAddEndOffsetMetricForThreadWhenPartitionsAssigned() {
    // when:
    final var callback = endOffsetsPoller.addForThread(THREAD_ID);
    callback.onPartitionsAssigned(List.of(PARTITION1, PARTITION2));

    // then:
    verify(metrics, times(2))
        .addMetric(metricNameCaptor.capture(), any(MetricValueProvider.class));
    assertThat(metricNameCaptor.getAllValues(), contains(
        metricName(THREAD_ID, PARTITION1),
        metricName(THREAD_ID, PARTITION2))
    );
  }

  @Test
  public void shouldRemoveEndOffsetMetricWhenPartitionsRevoked() {
    // given:
    final var callback = endOffsetsPoller.addForThread(THREAD_ID);
    callback.onPartitionsAssigned(List.of(PARTITION1, PARTITION2));

    // when:
    callback.onPartitionsRevoked(List.of(PARTITION1));

    // then:
    verify(metrics).removeMetric(metricName(THREAD_ID, PARTITION1));
  }

  @Test
  public void shouldRemoveEndOffsetMetricForThread() {
    // given:
    final var callback = endOffsetsPoller.addForThread(THREAD_ID);
    callback.onPartitionsAssigned(List.of(PARTITION1));

    // when:
    callback.close();

    // then:
    verify(metrics).removeMetric(metricName(THREAD_ID, PARTITION1));
  }

  @Test
  public void shouldPollAllEndOffsetsForThread() {
    // given:
    final var result = completedOffsetListing(Map.of(
        PARTITION1, new ListOffsetsResultInfo(123L, 100L, Optional.empty()),
        PARTITION2, new ListOffsetsResultInfo(456L, 200L, Optional.empty())
    ));
    when(adminClient.listOffsets(anyMap())).thenReturn(result);
    final var callback = endOffsetsPoller.addForThread(THREAD_ID);
    callback.onPartitionsAssigned(List.of(PARTITION1, PARTITION2));
    verify(metrics, times(2)).addMetric(any(), valueProviderCaptor.capture());
    verify(executor).scheduleAtFixedRate(taskCaptor.capture(), eq(0L), anyLong(), any());
    final var task = taskCaptor.getValue();

    // when:
    task.run();

    // then:
    final List<MetricValueProvider<Long>> providers = valueProviderCaptor.getAllValues();
    assertThat(providers.get(0), instanceOf(Gauge.class));
    assertThat(((Gauge<Long>) providers.get(0)).value(null, 0L), equalTo(123L));
    assertThat(providers.get(1), instanceOf(Gauge.class));
    assertThat(((Gauge<Long>) providers.get(1)).value(null, 0L), equalTo(456L));
  }

  @Test
  public void shouldPollEndOffsetsForMultipleThreads() {
    // given:
    final var result = completedOffsetListing(Map.of(
        PARTITION1, new ListOffsetsResultInfo(123L, 100L, Optional.empty()),
        PARTITION2, new ListOffsetsResultInfo(456L, 200L, Optional.empty())
    ));
    when(adminClient.listOffsets(anyMap())).thenReturn(result);
    final var callback = endOffsetsPoller.addForThread(THREAD_ID);
    callback.onPartitionsAssigned(List.of(PARTITION1));
    final var callback2 = endOffsetsPoller.addForThread("StreamThread-1");
    callback2.onPartitionsAssigned(List.of(PARTITION2));
    verify(metrics, times(2)).addMetric(any(), valueProviderCaptor.capture());
    verify(executor).scheduleAtFixedRate(taskCaptor.capture(), eq(0L), anyLong(), any());
    final var task = taskCaptor.getValue();

    // when:
    task.run();

    // then:
    final List<MetricValueProvider<Long>> providers = valueProviderCaptor.getAllValues();
    assertThat(providers.get(0), instanceOf(Gauge.class));
    assertThat(((Gauge<Long>) providers.get(0)).value(null, 0L), equalTo(123L));
    assertThat(providers.get(1), instanceOf(Gauge.class));
    assertThat(((Gauge<Long>) providers.get(1)).value(null, 0L), equalTo(456L));
  }

  private MetricName metricName(final String thread, final TopicPartition tp) {
    return new MetricName("end-offset", "responsive.streams", "",
        Map.of(
            "thread", thread,
            "topic", tp.topic(),
            "partition", Integer.toString(tp.partition())
        )
    );
  }

  private ListOffsetsResult completedOffsetListing(
      final Map<TopicPartition, ListOffsetsResultInfo> result
  ) {
    final Map<TopicPartition, KafkaFuture<ListOffsetsResultInfo>> futures = result.entrySet()
        .stream()
        .collect(Collectors.toMap(
            Entry::getKey,
            e -> {
              final KafkaFutureImpl<ListOffsetsResultInfo> future = new KafkaFutureImpl<>();
              future.complete(e.getValue());
              return future;
            }
        ));
    return new ListOffsetsResult(futures);
  }
}