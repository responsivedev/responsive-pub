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

package dev.responsive.kafka.clients;

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

import dev.responsive.kafka.clients.EndOffsetsPoller.Factories;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricValueProvider;
import org.apache.kafka.common.metrics.Metrics;
import org.junit.jupiter.api.AfterEach;
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
  private KafkaConsumer<byte[], byte[]> consumer;
  @Mock
  private Factories factories;
  @Mock
  private Metrics metrics;
  @Mock
  private ScheduledExecutorService executor;
  @Captor
  private ArgumentCaptor<MetricName> metricNameCaptor;
  @Captor
  private ArgumentCaptor<MetricValueProvider<Long>> valueProviderCaptor;
  @Captor
  private ArgumentCaptor<Runnable> taskCaptor;
  private EndOffsetsPoller endOffsetsPoller;

  @BeforeEach
  public void setup() {
    lenient().when(factories.createConsumer(anyMap())).thenReturn(consumer);
    lenient().when(factories.createExecutor()).thenReturn(executor);
    endOffsetsPoller = new EndOffsetsPoller(CONFIGS, metrics, factories);
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
        new MetricName("end-offset", "responsive.streams", "",
            Map.of(
                "thread", "StreamThread-0",
                "topic", "alice",
                "partition", "1"
            )
        ),
        new MetricName("end-offset", "responsive.streams", "",
            Map.of(
                "thread", "StreamThread-0",
                "topic", "bob",
                "partition", "2"
            )
    )));
  }

  @Test
  public void shouldRemoveEndOffsetMetricForThread() {
    // given:
    final var callback = endOffsetsPoller.addForThread(THREAD_ID);
    callback.onPartitionsAssigned(List.of(PARTITION1, PARTITION2));

    // when:
    callback.onPartitionsRevoked(List.of(PARTITION1));

    // then:
    verify(metrics).removeMetric(metricNameCaptor.capture());
  }

  @Test
  public void shouldPollEndOffsets() {
    // given:
    when(consumer.endOffsets(any())).thenReturn(Map.of(
        PARTITION1, 123L,
        PARTITION2, 456L
    ));
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
}