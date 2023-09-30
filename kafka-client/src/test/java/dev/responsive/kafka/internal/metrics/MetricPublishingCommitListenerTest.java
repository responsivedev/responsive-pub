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
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import dev.responsive.kafka.internal.clients.OffsetRecorder;
import dev.responsive.kafka.internal.clients.ResponsiveProducer.Listener;
import dev.responsive.kafka.internal.metrics.MetricPublishingCommitListener;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.Metrics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class MetricPublishingCommitListenerTest {
  private static final String THREAD_ID = "StreamThread-0";
  private static final String GROUP = "foo";
  private static final TopicPartition PARTITION1 = new TopicPartition("blimp", 1);
  private static final TopicPartition PARTITION2 = new TopicPartition("airplane", 2);

  @Mock
  private Metrics metrics;
  @Captor
  private ArgumentCaptor<MetricName> nameCaptor;
  @Captor
  private ArgumentCaptor<Gauge<Long>> metricCaptor;

  private final OffsetRecorder offsetRecorder = new OffsetRecorder(true);

  private MetricPublishingCommitListener listener;

  @BeforeEach
  public void setup() {
    listener = new MetricPublishingCommitListener(metrics, THREAD_ID, GROUP, offsetRecorder);
  }

  @Test
  public void shouldReportCommittedOffsets() {
    // given:
    listener.onPartitionsAssigned(List.of(PARTITION1, PARTITION2));
    final Listener recorderProducerListener = offsetRecorder.getProducerListener();
    recorderProducerListener.onSendOffsetsToTransaction(
        Map.of(PARTITION1, new OffsetAndMetadata(123L), PARTITION2, new OffsetAndMetadata(345L)),
        GROUP
    );

    // when:
    recorderProducerListener.onCommit();

    // then:
    verify(metrics, times(2)).addMetric(
        nameCaptor.capture(), metricCaptor.capture()
    );
    final var values = metricCaptor.getAllValues();
    final var names = nameCaptor.getAllValues();
    final var nameToValue = IntStream.range(0, names.size()).boxed()
        .collect(Collectors.toMap(names::get, values::get));
    assertThat(nameToValue.get(getName(PARTITION1)).value(null, 0), is(123L));
    assertThat(nameToValue.get(getName(PARTITION2)).value(null, 0), is(345L));
  }

  @Test
  public void shouldReportUnknownCommittedOffsets() {
    // when:
    listener.onPartitionsAssigned(List.of(PARTITION1, PARTITION2));

    // then:
    verify(metrics, times(2)).addMetric(
        nameCaptor.capture(), metricCaptor.capture()
    );
    final var values = metricCaptor.getAllValues();
    final var names = nameCaptor.getAllValues();
    final var nameToValue = IntStream.range(0, names.size()).boxed()
        .collect(Collectors.toMap(names::get, values::get));
    assertThat(nameToValue.get(getName(PARTITION1)).value(null, 0), is(-1L));
    assertThat(nameToValue.get(getName(PARTITION2)).value(null, 0), is(-1L));
  }

  @Test
  public void shouldCleanupCommittedOffsetOnRevoke() {
    // given:
    listener.onPartitionsAssigned(List.of(PARTITION1, PARTITION2));
    final Listener recorderProducerListener = offsetRecorder.getProducerListener();
    recorderProducerListener.onSendOffsetsToTransaction(
        Map.of(PARTITION1, new OffsetAndMetadata(123L), PARTITION2, new OffsetAndMetadata(345L)),
        GROUP
    );
    recorderProducerListener.onCommit();

    // when:
    listener.onPartitionsRevoked(List.of(PARTITION1));

    // then:
    verify(metrics).removeMetric(nameCaptor.capture());
    assertThat(nameCaptor.getValue(), is(getName(PARTITION1)));
  }

  @Test
  public void shouldAddCommittedOffsetMetricOnAssign() {
    // given:
    listener.onPartitionsAssigned(List.of(PARTITION1, PARTITION2));
    final Listener recorderProducerListener = offsetRecorder.getProducerListener();
    recorderProducerListener.onSendOffsetsToTransaction(
        Map.of(PARTITION1, new OffsetAndMetadata(123L), PARTITION2, new OffsetAndMetadata(345L)),
        GROUP
    );

    // when:
    recorderProducerListener.onCommit();

    // then:
    verify(metrics, times(2))
        .addMetric(nameCaptor.capture(), any(Gauge.class));
    assertThat(
        nameCaptor.getAllValues(),
        containsInAnyOrder(getName(PARTITION1), getName(PARTITION2))
    );
  }

  @Test
  public void shouldCleanupMetricsOnClose() {
    // given:
    listener.onPartitionsAssigned(List.of(PARTITION1, PARTITION2));
    final Listener recorderProducerListener = offsetRecorder.getProducerListener();
    recorderProducerListener.onSendOffsetsToTransaction(
        Map.of(PARTITION1, new OffsetAndMetadata(123L), PARTITION2, new OffsetAndMetadata(345L)),
        GROUP
    );
    recorderProducerListener.onCommit();

    // when:
    listener.close();

    // then:
    verify(metrics, times(2)).removeMetric(nameCaptor.capture());
    assertThat(
        nameCaptor.getAllValues(),
        containsInAnyOrder(getName(PARTITION1), getName(PARTITION2))
    );
  }

  private MetricName getName(final TopicPartition tp) {
    return new MetricName(
        "committed-offset", "responsive.streams", "",
        Map.of(
            "thread", THREAD_ID,
            "topic", tp.topic(),
            "partition", Integer.toString(tp.partition()),
            "consumerGroup", GROUP
        )
    );
  }
}