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

package dev.responsive.kafka.internal.clients;


import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import dev.responsive.kafka.api.async.internals.AsyncThreadPoolRegistry;
import dev.responsive.kafka.api.config.CompatibilityMode;
import dev.responsive.kafka.internal.clients.ResponsiveKafkaClientSupplier.Factories;
import dev.responsive.kafka.internal.metrics.EndOffsetsPoller;
import dev.responsive.kafka.internal.metrics.MetricPublishingCommitListener;
import dev.responsive.kafka.internal.metrics.ResponsiveMetrics;
import dev.responsive.kafka.internal.stores.ResponsiveStoreRegistry;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.BytesSerializer;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.StreamsConfig;
import org.hamcrest.Matchers;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
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

  private static final Map<String, Object> RESTORE_CONSUMER_CONFIGS = configsWithOverrides(
      Map.of(
          ProducerConfig.CLIENT_ID_CONFIG, "foo-StreamThread-0-restore-consumer"
      )
  );

  @Mock
  private Factories factories;
  @Mock
  private ResponsiveMetrics metrics;
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
  @Captor
  private ArgumentCaptor<List<ResponsiveProducer.Listener>> producerListenerCaptor;
  @Captor
  private ArgumentCaptor<List<ResponsiveConsumer.Listener>> consumerListenerCaptor;
  private final OffsetRecorder offsetRecorder = new OffsetRecorder(true, "thread");
  private ResponsiveKafkaClientSupplier supplier;

  private final ResponsiveStoreRegistry storeRegistry = new ResponsiveStoreRegistry();

  @BeforeEach
  @SuppressWarnings("unchecked")
  public void setup() {
    when(factories.createEndOffsetPoller(any(), any(), any())).thenReturn(endOffsetsPoller);
    lenient().when(endOffsetsPoller.addForThread(any())).thenReturn(consumerEndOffsetsPollListener);
    lenient().when(wrapped.getConsumer(any())).thenReturn(wrappedConsumer);
    lenient().when(wrapped.getProducer(any())).thenReturn(wrappedProducer);
    lenient().when(
        factories.createResponsiveProducer(any(), (ResponsiveProducer<byte[], byte[]>) any(), any())
    ).thenReturn(responsiveProducer);
    lenient().when(factories.createResponsiveConsumer(
        any(), (ResponsiveConsumer<byte[], byte[]>) any(), any())
    ).thenReturn(responsiveConsumer);
    lenient().when(factories.createMetricsPublishingCommitListener(any(), any(), any()))
        .thenReturn(commitMetricListener);
    lenient().when(factories.createOffsetRecorder(anyBoolean(), any())).thenReturn(offsetRecorder);

    supplier = supplier(CONFIGS, CompatibilityMode.FULL);
  }

  @Test
  public void shouldWrapProducerIfAlos() {
    // given:
    final var config = configsWithOverrides(
        PRODUCER_CONFIGS,
        Map.of(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.AT_LEAST_ONCE));
    final var supplier = supplier(config, CompatibilityMode.FULL);

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
    final var supplier = supplier(config, CompatibilityMode.FULL);

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
        metrics, "StreamThread-0", offsetRecorder);
  }

  @Test
  public void shouldAddMetricPublishingCommitListenerToConsumer() {
    // when:
    supplier.getConsumer(CONSUMER_CONFIGS);

    // then:
    verify(factories).createResponsiveConsumer(
        any(), any(), consumerListenerCaptor.capture());
    assertThat(consumerListenerCaptor.getValue(), Matchers.hasItem(commitMetricListener));
    verify(factories).createMetricsPublishingCommitListener(
        metrics, "StreamThread-0", offsetRecorder);
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
        metrics, "StreamThread-0", offsetRecorder);
    verify(factories, times(1)).createMetricsPublishingCommitListener(
        metrics, "StreamThread-1", offsetRecorder);
  }

  @Test
  public void shouldAddEndOffsetsPollerListeners() {
    // when:
    supplier.getConsumer(CONSUMER_CONFIGS);

    // then:
    verify(factories).createResponsiveConsumer(
        any(), any(), consumerListenerCaptor.capture());
    assertThat(consumerListenerCaptor.getValue(), Matchers.hasItem(consumerEndOffsetsPollListener));
  }

  @Test
  public void shouldCloseMetricPublishingCommitListenerWhenNoRefs() {
    // given:
    supplier.getConsumer(CONSUMER_CONFIGS);
    supplier.getProducer(PRODUCER_CONFIGS);

    // then:
    verify(factories).createResponsiveConsumer(
        any(), any(), consumerListenerCaptor.capture());
    consumerListenerCaptor.getValue().forEach(ResponsiveConsumer.Listener::onClose);
    verify(commitMetricListener, times(0)).close();
    verify(factories).createResponsiveProducer(any(), any(), producerListenerCaptor.capture());
    producerListenerCaptor.getValue().forEach(ResponsiveProducer.Listener::onClose);
    verify(commitMetricListener).close();
  }

  @Test
  public void shouldCreateGlobalAndRestoreConsumerInFullCompatibilityMode() {
    // given:
    supplier = supplier(CONFIGS, CompatibilityMode.FULL);

    // when:
    supplier.getRestoreConsumer(new HashMap<>(RESTORE_CONSUMER_CONFIGS));
    supplier.getGlobalConsumer(new HashMap<>(PRODUCER_CONFIGS));

    // then:
    verify(factories, Mockito.atLeastOnce()).createGlobalConsumer(any(), any(), any());
    verify(factories, Mockito.atLeastOnce()).createRestoreConsumer(
        any(),
        any(),
        any(),
        anyBoolean()
    );
  }

  @Test
  public void shouldNotCreateGlobalOrRestoreConsumerInMetricsCompatibilityMode() {
    // given:
    supplier = supplier(CONFIGS, CompatibilityMode.METRICS_ONLY);

    // when:
    supplier.getRestoreConsumer(new HashMap<>(CONSUMER_CONFIGS));
    supplier.getGlobalConsumer(new HashMap<>(PRODUCER_CONFIGS));

    // then:
    verify(factories, Mockito.never()).createGlobalConsumer(any(), any(), any());
    verify(factories, Mockito.never()).createRestoreConsumer(any(), any(), any(), anyBoolean());
  }

  @NotNull
  private ResponsiveKafkaClientSupplier supplier(
      final Map<String, Object> configs,
      final CompatibilityMode compat
  ) {
    return new ResponsiveKafkaClientSupplier(
        factories,
        wrapped,
        new StreamsConfig(configs),
        storeRegistry,
        metrics,
        compat,
        false
    );
  }

  private static Map<String, Object> configsWithOverrides(final Map<String, Object> overrides) {
    return configsWithOverrides(CONFIGS, overrides);
  }

  private static Map<String, Object> configsWithOverrides(
      final Map<String, Object> configs,
      final Map<String, Object> overrides
  ) {
    final var intermediate = new HashMap<String, Object>();
    intermediate.putAll(configs);
    intermediate.putAll(overrides);
    intermediate.put(
        "__internal.responsive.async.thread.pool.registry__", new AsyncThreadPoolRegistry(
            2,
            5,
            5,
            mock(ResponsiveMetrics.class)
        )
    );
    return Map.copyOf(intermediate);
  }
}
