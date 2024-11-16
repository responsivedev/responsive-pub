/*
 * Copyright 2024 Responsive Computing, Inc.
 *
 * This source code is licensed under the Responsive Software License Agreement v1.0 available at
 *
 * https://www.responsive.dev/legal/responsive-bsl-10
 *
 * This software requires a valid Commercial License Key for production use. Trial and commercial
 * licenses can be obtained at https://www.responsive.dev/sdk/get-started.
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
import java.util.Collections;
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
  private static final String CLIENT = "foo-bar";
  private static final TopicPartition PARTITION1 = new TopicPartition("blimp", 1);
  private static final TopicPartition PARTITION2 = new TopicPartition("airplane", 2);

  @Mock
  private Metrics metrics;
  @Captor
  private ArgumentCaptor<MetricName> nameCaptor;
  @Captor
  private ArgumentCaptor<Gauge<Long>> metricCaptor;

  private final OffsetRecorder offsetRecorder = new OffsetRecorder(true, "thread");

  private MetricPublishingCommitListener listener;

  @BeforeEach
  public void setup() {
    final ResponsiveMetrics responsiveMetrics = new ResponsiveMetrics(metrics);
    responsiveMetrics.initializeTags(
        GROUP, CLIENT, new ClientVersionMetadata("1", "abc", "2", "dfe"), Collections.emptyMap());
    listener = new MetricPublishingCommitListener(responsiveMetrics, THREAD_ID, offsetRecorder);
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
        "committed-offset",
        "topic-metrics",
        "The latest committed offset of this topic partition",
        Map.of(
            "thread-id", THREAD_ID,
            "topic", tp.topic(),
            "partition", Integer.toString(tp.partition()),
            "consumer-group", GROUP,
            "streams-application-id", GROUP,
            "streams-client-id", CLIENT,
            "responsive-version", "1",
            "responsive-commit-id", "abc",
            "streams-version", "2",
            "streams-commit-id", "dfe"
        )
    );
  }
}