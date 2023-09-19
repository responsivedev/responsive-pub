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
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import dev.responsive.kafka.clients.ResponsiveKafkaClientSupplier.Factories;
import dev.responsive.kafka.store.ResponsiveStoreRegistry;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.BytesSerializer;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.StreamsConfig;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ResponsiveKafkaClientSupplierTest {
  private static final Map<String, Object> CONFIGS = Map.of(
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class,
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, BytesSerializer.class,
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, BytesSerializer.class,
      StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2,
      StreamsConfig.APPLICATION_ID_CONFIG, "appid",
      StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"
  );
  private static final Map<String, Object> PRODUCER_CONFIGS = configsWithOverrides(
      Map.of(
          ProducerConfig.CLIENT_ID_CONFIG, "foo-StreamThread-0-producer"
      )
  );

  private static final Map<String, Object> CONSUMER_CONFIGS = configsWithOverrides(
      Map.of(
          ProducerConfig.CLIENT_ID_CONFIG, "foo-StreamThread-0-consumer"
      )
  );

  @Mock
  private Factories factories;
  @Mock
  private Metrics metrics;
  @Mock
  private EndOffsetsPoller.Listener consumerEndOffsetsPollListener;
  @Mock
  private EndOffsetsPoller endOffsetsPoller;
  @Mock
  private KafkaClientSupplier wrapped;
  @Mock
  private Producer<byte[], byte[]> wrappedProducer;
  @Mock
  private Consumer<byte[], byte[]> wrappedConsumer;
  @Mock
  private ResponsiveProducer<byte[], byte[]> responsiveProducer;
  @Mock
  private ResponsiveConsumer<byte[], byte[]> responsiveConsumer;
  @Mock
  private MetricPublishingCommitListener commitMetricListener;
  @Mock
  private ResponsiveProducer.Listener commitMetricProducerListener;
  @Captor
  private ArgumentCaptor<List<ResponsiveProducer.Listener>> producerListenerCaptor;
  @Captor
  private ArgumentCaptor<List<ResponsiveConsumer.Listener>> consumerListenerCaptor;
  private final OffsetRecorder offsetRecorder = new OffsetRecorder(true);
  private ResponsiveKafkaClientSupplier supplier;

  private final ResponsiveStoreRegistry storeRegistry = new ResponsiveStoreRegistry();

  @BeforeEach
  @SuppressWarnings("unchecked")
  public void setup() {
    when(factories.createEndOffsetPoller(any(), any())).thenReturn(endOffsetsPoller);
    lenient().when(endOffsetsPoller.addForThread(any())).thenReturn(consumerEndOffsetsPollListener);
    lenient().when(wrapped.getConsumer(any())).thenReturn(wrappedConsumer);
    lenient().when(wrapped.getProducer(any())).thenReturn(wrappedProducer);
    lenient().when(
        factories.createResponsiveProducer(any(), (ResponsiveProducer<byte[], byte[]>) any(), any())
    ).thenReturn(responsiveProducer);
    lenient().when(
        factories.createResponsiveConsumer(any(), (ResponsiveConsumer<byte[], byte[]>) any(), any())
    ).thenReturn(responsiveConsumer);
    lenient().when(factories.createMetricsPublishingCommitListener(any(), any(), any(), any()))
        .thenReturn(commitMetricListener);
    lenient().when(factories.createOffsetRecorder(anyBoolean())).thenReturn(offsetRecorder);

    supplier = new ResponsiveKafkaClientSupplier(
        factories,
        wrapped,
        new StreamsConfig(CONFIGS),
        storeRegistry,
        metrics
    );
  }

  @Test
  public void shouldWrapProducerIfAlos() {
    // given:
    final var config = configsWithOverrides(
        PRODUCER_CONFIGS,
        Map.of(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.AT_LEAST_ONCE));
    final var supplier = new ResponsiveKafkaClientSupplier(
        factories,
        wrapped,
        new StreamsConfig(config),
        storeRegistry,
        metrics
    );

    // when:
    final var producer = supplier.getProducer(config);

    // then:
    assertThat(producer, is(responsiveProducer));
  }

  @Test
  public void shouldWrapConsumerIfAlos() {
    // given:
    final var config = configsWithOverrides(
        CONSUMER_CONFIGS,
        Map.of(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.AT_LEAST_ONCE));
    final var supplier = new ResponsiveKafkaClientSupplier(
        factories,
        wrapped,
        new StreamsConfig(config),
        storeRegistry,
        metrics
    );

    // when:
    final var consumer = supplier.getConsumer(config);

    // then:
    assertThat(consumer, is(responsiveConsumer));
  }

  @Test
  public void shoudWrapProducer() {
    // when:
    final var producer = supplier.getProducer(PRODUCER_CONFIGS);

    // then:
    assertThat(producer, Matchers.is(responsiveProducer));
  }

  @Test
  public void shouldAddOffsetRecorderCommitListenerToProducer() {
    // when:
    supplier.getProducer(PRODUCER_CONFIGS);

    // then:
    verify(factories).createResponsiveProducer(any(), any(), producerListenerCaptor.capture());
    assertThat(
        producerListenerCaptor.getValue(),
        Matchers.hasItem(offsetRecorder.getProducerListener())
    );
    verify(factories).createMetricsPublishingCommitListener(
        metrics, "StreamThread-0", "appid", offsetRecorder);
  }

  @Test
  public void shouldAddMetricPublishingCommitListenerToConsumer() {
    // when:
    supplier.getConsumer(CONSUMER_CONFIGS);

    // then:
    verify(factories).createResponsiveConsumer(any(), any(), consumerListenerCaptor.capture());
    assertThat(consumerListenerCaptor.getValue(), Matchers.hasItem(commitMetricListener));
    verify(factories).createMetricsPublishingCommitListener(
        metrics, "StreamThread-0", "appid", offsetRecorder);
  }

  @Test
  public void shouldMatchMetricPublishingCommitListenerForThread() {
    // given:
    supplier.getProducer(PRODUCER_CONFIGS);

    // when:
    supplier.getConsumer(CONSUMER_CONFIGS);
    final Map<String, Object> withOverrides = configsWithOverrides(
        CONSUMER_CONFIGS,
        Map.of(ConsumerConfig.CLIENT_ID_CONFIG, "foo-StreamThread-1-consumer")
    );
    supplier.getConsumer(withOverrides);

    // then:
    verify(factories, times(1)).createMetricsPublishingCommitListener(
        metrics, "StreamThread-0", "appid", offsetRecorder);
    verify(factories, times(1)).createMetricsPublishingCommitListener(
        metrics, "StreamThread-1", "appid", offsetRecorder);
  }

  @Test
  public void shouldAddEndOffsetsPollerListeners() {
    // when:
    supplier.getConsumer(CONSUMER_CONFIGS);

    // then:
    verify(factories).createResponsiveConsumer(any(), any(), consumerListenerCaptor.capture());
    assertThat(consumerListenerCaptor.getValue(), Matchers.hasItem(consumerEndOffsetsPollListener));
  }

  @Test
  public void shouldCloseMetricPublishingCommitListenerWhenNoRefs() {
    // given:
    supplier.getConsumer(CONSUMER_CONFIGS);
    supplier.getProducer(PRODUCER_CONFIGS);

    // then:
    verify(factories).createResponsiveConsumer(any(), any(), consumerListenerCaptor.capture());
    consumerListenerCaptor.getValue().forEach(ResponsiveConsumer.Listener::onClose);
    verify(commitMetricListener, times(0)).close();
    verify(factories).createResponsiveProducer(any(), any(), producerListenerCaptor.capture());
    producerListenerCaptor.getValue().forEach(ResponsiveProducer.Listener::onClose);
    verify(commitMetricListener).close();
  }

  private static Map<String, Object> configsWithOverrides(final Map<String, Object> overrides) {
    return configsWithOverrides(CONFIGS, overrides);
  }

  private static Map<String, Object> configsWithOverrides(
      final Map<String, Object> configs,
      final Map<String, Object> overrides) {
    final var intermediate = new HashMap<String, Object>();
    intermediate.putAll(configs);
    intermediate.putAll(overrides);
    return Map.copyOf(intermediate);
  }
}
