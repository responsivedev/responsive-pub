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

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsContext;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of KafkaClientSupplier for Responsive applications. The supplier returns
 * wrapped kafka producers/consumers that are instrumented to work with the responsive platform
 * by, for example, emitting metrics useful for debug and scaling.
 *
 * Synchronization: This class is read/written during client initialization and close (via the
 * close callbacks on the returned clients). As these calls are not on the performance path we
 * rely on coarse-grained locks around sections reading/writing shared state.
 */
public final class ResponsiveKafkaClientSupplier implements KafkaClientSupplier, Configurable {
  private static final Logger LOGGER = LoggerFactory.getLogger(ResponsiveKafkaClientSupplier.class);
  private final SharedListeners sharedListeners = new SharedListeners();
  private final KafkaClientSupplier wrapped;
  private final Factories factories;
  private boolean eosV2 = false;
  private boolean configured = false;
  private Metrics metrics;
  private EndOffsetsPoller endOffsetsPoller;

  public ResponsiveKafkaClientSupplier() {
    this(new Factories() {}, new DefaultKafkaClientSupplier());
  }

  ResponsiveKafkaClientSupplier(final Factories factories, final KafkaClientSupplier wrapped) {
    this.factories = factories;
    this.wrapped = wrapped;
  }

  @Override
  public void configure(final Map<String, ?> configs) {
    LOGGER.trace(
        "Configuring the client supplier. Got configs: {}",
        configs.entrySet().stream()
            .map(e -> e.getKey() + ":" + e.getValue())
            .collect(Collectors.joining("\n"))
    );
    eosV2 = (configs.get(StreamsConfig.PROCESSING_GUARANTEE_CONFIG))
        .equals(StreamsConfig.EXACTLY_ONCE_V2);
    final JmxReporter jmxReporter = new JmxReporter();
    jmxReporter.configure(configs);
    final MetricsContext metricsContext
        = new KafkaMetricsContext("dev.responsive", new HashMap<>());
    metrics = factories.createMetrics(
        new MetricConfig(),
        Collections.singletonList(jmxReporter),
        Time.SYSTEM,
        metricsContext
    );
    endOffsetsPoller = factories.createEndOffsetPoller(configs, metrics);
    configured = true;
  }

  @Override
  public Admin getAdmin(final Map<String, Object> config) {
    return wrapped.getAdmin(config);
  }

  @Override
  public Producer<byte[], byte[]> getProducer(final Map<String, Object> config) {
    if (!configured) {
      throw new IllegalStateException("must be configured before supplying clients");
    }
    if (!eosV2) {
      return wrapped.getProducer(config);
    }
    LOGGER.info("Creating responsive producer");
    final String tid = threadIdFromProducerConfig(config);
    final ListenersForThread tc = sharedListeners.getAndMaybeInitListenersForThread(
        tid,
        metrics,
        endOffsetsPoller,
        factories
    );
    return factories.createResponsiveProducer(
        (String) config.get(CommonClientConfigs.CLIENT_ID_CONFIG),
        wrapped.getProducer(config),
        Collections.unmodifiableList(
            Arrays.asList(
                tc.commitListener,
                new CloseListener(tid)
            )
        )
    );
  }

  @Override
  public Consumer<byte[], byte[]> getConsumer(final Map<String, Object> config) {
    if (!configured) {
      throw new IllegalStateException("must be configured before supplying clients");
    }
    if (!eosV2) {
      return wrapped.getConsumer(config);
    }
    LOGGER.info("Creating responsive consumer");
    final String tid = threadIdFromConsumerConfig(config);
    final ListenersForThread tc = sharedListeners.getAndMaybeInitListenersForThread(
        tid,
        metrics,
        endOffsetsPoller,
        factories
    );
    // TODO: the end offsets poller call is kind of heavy for a synchronized block
    return factories.createResponsiveConsumer(
        (String) config.get(CommonClientConfigs.CLIENT_ID_CONFIG),
        wrapped.getConsumer(config),
        List.of(
            tc.commitListener,
            tc.endOffsetsPollerListener,
            new CloseListener(tid)
        )
    );
  }

  @Override
  public Consumer<byte[], byte[]> getRestoreConsumer(final Map<String, Object> config) {
    return wrapped.getRestoreConsumer(config);
  }

  @Override
  public Consumer<byte[], byte[]> getGlobalConsumer(final Map<String, Object> config) {
    return wrapped.getGlobalConsumer(config);
  }

  private String threadIdFromProducerConfig(final Map<String, Object> config) {
    final String clientId = (String) config.get(CommonClientConfigs.CLIENT_ID_CONFIG);
    LOGGER.info("Found producer client ID {}", clientId);
    final var regex = Pattern.compile(".*-(StreamThread-\\d+)-producer");
    final var match = regex.matcher(clientId);
    if (!match.find()) {
      throw new RuntimeException("unexpected client id " + clientId);
    }
    return match.group(1);
  }

  private String threadIdFromConsumerConfig(final Map<String, Object> config) {
    final String clientId = (String) config.get(CommonClientConfigs.CLIENT_ID_CONFIG);
    LOGGER.info("Found consumer client ID {}", clientId);
    final var regex = Pattern.compile(".*-(StreamThread-\\d+)-consumer$");
    final var match = regex.matcher(clientId);
    if (!match.find()) {
      throw new RuntimeException("unexpected client id " + clientId);
    }
    return match.group(1);
  }

  class CloseListener implements ResponsiveConsumer.Listener, ResponsiveProducer.Listener {
    private final String threadId;

    private CloseListener(final String threadId) {
      this.threadId = threadId;
    }

    @Override
    public void onClose() {
      sharedListeners.derefListenersForThread(threadId);
    }
  }

  private static class SharedListeners {
    private final Map<String, ReferenceCounted<ListenersForThread>> threadListeners
        = new HashMap<>();

    private synchronized ListenersForThread getAndMaybeInitListenersForThread(
        final String threadId,
        final Metrics metrics,
        final EndOffsetsPoller endOffsetsPoller,
        final Factories factories
    ) {
      if (threadListeners.containsKey(threadId)) {
        final var tl = threadListeners.get(threadId);
        tl.ref();
        return tl.getVal();
      }
      final var tl = new ReferenceCounted<>(new ListenersForThread(
          threadId,
          factories.createMetricsPublishingCommitListener(metrics, threadId),
          endOffsetsPoller.addForThread(threadId)
      ));
      threadListeners.put(threadId, tl);
      return tl.getVal();
    }

    private synchronized void derefListenersForThread(final String threadId) {
      if (threadListeners.get(threadId).deref()) {
        threadListeners.remove(threadId);
      }
    }
  }

  private static class ListenersForThread implements Closeable {
    final String threadId;
    final MetricPublishingCommitListener commitListener;
    final EndOffsetsPoller.Listener endOffsetsPollerListener;

    public ListenersForThread(
        final String threadId,
        final MetricPublishingCommitListener commitListener,
        final EndOffsetsPoller.Listener endOffsetsPollerListener) {
      this.threadId = threadId;
      this.commitListener = commitListener;
      this.endOffsetsPollerListener = endOffsetsPollerListener;
    }

    public void close() {
      commitListener.close();
      endOffsetsPollerListener.close();
    }
  }

  private static class ReferenceCounted<T extends Closeable> {
    final T val;
    int count;

    private ReferenceCounted(final T val) {
      this.val = val;
      this.count = 1;
    }

    private void ref() {
      LOGGER.info("bumping ref count to {}", count + 1);
      count += 1;
    }

    private boolean deref() {
      LOGGER.info("reducing ref count to {}", count - 1);
      count -= 1;
      if (count == 0) {
        LOGGER.info("closing referred value");
        try {
          val.close();
        } catch (final IOException e) {
          throw new RuntimeException(e);
        }
        return true;
      }
      return false;
    }

    private T getVal() {
      return val;
    }
  }

  interface Factories {
    default EndOffsetsPoller createEndOffsetPoller(
        final Map<String, ?> config,
        final Metrics metrics
    ) {
      return new EndOffsetsPoller(config, metrics);
    }

    default Metrics createMetrics(
        final MetricConfig config,
        final List<MetricsReporter> reporters,
        final Time time,
        final MetricsContext ctx
    ) {
      return new Metrics(config, reporters, time, ctx);
    }

    default <K, V> ResponsiveProducer<K, V> createResponsiveProducer(
        final String clientId,
        final Producer<K, V> wrapped,
        final List<ResponsiveProducer.Listener> listeners
    ) {
      return new ResponsiveProducer<>(clientId, wrapped, listeners);
    }

    default <K, V> ResponsiveConsumer<K, V> createResponsiveConsumer(
        final String clientId,
        final Consumer<K, V> wrapped,
        final List<ResponsiveConsumer.Listener> listeners) {
      return new ResponsiveConsumer<>(clientId, wrapped, listeners);
    }

    default <K, V> MetricPublishingCommitListener createMetricsPublishingCommitListener(
        final Metrics metrics,
        final String threadId
    ) {
      return new MetricPublishingCommitListener(metrics, threadId);
    }
  }
}