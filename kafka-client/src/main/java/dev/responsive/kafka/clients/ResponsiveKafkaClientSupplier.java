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

import static org.apache.kafka.streams.StreamsConfig.AT_LEAST_ONCE;
import static org.apache.kafka.streams.StreamsConfig.EXACTLY_ONCE;

import dev.responsive.kafka.store.ResponsiveStoreRegistry;
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
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.Producer;
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
 * <p>
 * Synchronization: This class is read/written during client initialization and close (via the
 * close callbacks on the returned clients). As these calls are not on the performance path we
 * rely on coarse-grained locks around sections reading/writing shared state.
 */
public final class ResponsiveKafkaClientSupplier implements KafkaClientSupplier {
  private static final Logger LOG = LoggerFactory.getLogger(ResponsiveKafkaClientSupplier.class);
  private final SharedListeners sharedListeners = new SharedListeners();
  private final KafkaClientSupplier wrapped;
  private final ResponsiveStoreRegistry storeRegistry;
  private final Factories factories;
  private boolean configured = false;
  private Metrics metrics;
  private EndOffsetsPoller endOffsetsPoller;
  private String applicationId;
  private boolean eos = false;

  public ResponsiveKafkaClientSupplier(
      final Map<String, ?> configs,
      final ResponsiveStoreRegistry storeRegistry) {
    this(new Factories() {}, new DefaultKafkaClientSupplier(), configs, storeRegistry);
  }

  public ResponsiveKafkaClientSupplier(
      final KafkaClientSupplier clientSupplier,
      final Map<String, ?> configs,
      final ResponsiveStoreRegistry storeRegistry
  ) {
    this(new Factories() {}, clientSupplier, configs, storeRegistry);
  }

  ResponsiveKafkaClientSupplier(
      final Factories factories,
      final KafkaClientSupplier wrapped,
      final Map<String, ?> configs,
      final ResponsiveStoreRegistry storeRegistry
  ) {
    this.factories = factories;
    this.wrapped = wrapped;
    this.storeRegistry = storeRegistry;
    configure(configs);
  }

  @SuppressWarnings("deprecation")
  private void verifyNotEosV1(final StreamsConfig streamsConfig) {
    if (EXACTLY_ONCE.equals(streamsConfig.getString(StreamsConfig.PROCESSING_GUARANTEE_CONFIG))) {
      throw new IllegalStateException("Responsive driver can only be used with ALOS/EOS-V2");
    }
  }

  private void configure(final Map<String, ?> configs) {
    LOG.trace(
        "Configuring the client supplier. Got configs: {}",
        configs.entrySet().stream()
            .map(e -> e.getKey() + ":" + e.getValue())
            .collect(Collectors.joining("\n"))
    );
    final StreamsConfig streamsConfig = new StreamsConfig(configs);
    verifyNotEosV1(streamsConfig);
    eos = !(AT_LEAST_ONCE.equals(
        streamsConfig.getString(StreamsConfig.PROCESSING_GUARANTEE_CONFIG)));
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
    applicationId = (String) configs.get(StreamsConfig.APPLICATION_ID_CONFIG);
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
    LOG.info("Creating responsive producer");
    final String tid = threadIdFromProducerConfig(config);
    final ListenersForThread tc = sharedListeners.getAndMaybeInitListenersForThread(
        eos,
        tid,
        metrics,
        endOffsetsPoller,
        storeRegistry,
        factories
    );
    return factories.createResponsiveProducer(
        (String) config.get(CommonClientConfigs.CLIENT_ID_CONFIG),
        wrapped.getProducer(config),
        Collections.unmodifiableList(
            Arrays.asList(
                tc.offsetRecorder.getProducerListener(),
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
    LOG.info("Creating responsive consumer");
    final String tid = threadIdFromConsumerConfig(config);
    final ListenersForThread tc = sharedListeners.getAndMaybeInitListenersForThread(
        eos,
        tid,
        metrics,
        endOffsetsPoller,
        storeRegistry,
        factories
    );
    // TODO: the end offsets poller call is kind of heavy for a synchronized block
    return factories.createResponsiveConsumer(
        (String) config.get(CommonClientConfigs.CLIENT_ID_CONFIG),
        wrapped.getConsumer(config),
        List.of(
            tc.committedOffsetMetricListener,
            tc.offsetRecorder.getConsumerListener(),
            tc.endOffsetsPollerListener,
            new CloseListener(tid)
        )
    );
  }

  @Override
  public Consumer<byte[], byte[]> getRestoreConsumer(final Map<String, Object> config) {
    return new ResponsiveRestoreConsumer<>(
        wrapped.getRestoreConsumer(config),
        storeRegistry::getCommittedOffset
    );
  }

  @Override
  public Consumer<byte[], byte[]> getGlobalConsumer(final Map<String, Object> config) {
    config.put(ConsumerConfig.GROUP_ID_CONFIG, applicationId + "-global");
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);

    return new ResponsiveGlobalConsumer(
        config,
        wrapped.getGlobalConsumer(config),
        getAdmin(config)
    );
  }

  private String threadIdFromProducerConfig(final Map<String, Object> config) {
    final String clientId = (String) config.get(CommonClientConfigs.CLIENT_ID_CONFIG);
    LOG.info("Found producer client ID {}", clientId);
    final var regex = Pattern.compile(".*-(StreamThread-\\d+)-producer");
    final var match = regex.matcher(clientId);
    if (!match.find()) {
      throw new RuntimeException("unexpected client id " + clientId);
    }
    return match.group(1);
  }

  private String threadIdFromConsumerConfig(final Map<String, Object> config) {
    final String clientId = (String) config.get(CommonClientConfigs.CLIENT_ID_CONFIG);
    LOG.info("Found consumer client ID {}", clientId);
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
        final boolean eos,
        final String threadId,
        final Metrics metrics,
        final EndOffsetsPoller endOffsetsPoller,
        final ResponsiveStoreRegistry storeRegistry,
        final Factories factories
    ) {
      if (threadListeners.containsKey(threadId)) {
        final var tl = threadListeners.get(threadId);
        tl.ref();
        return tl.getVal();
      }
      final var offsetRecorder = factories.createOffsetRecorder(eos);
      final var tl = new ReferenceCounted<>(
          String.format("ListenersForThread(%s)", threadId),
          new ListenersForThread(
              threadId,
              offsetRecorder,
              factories.createMetricsPublishingCommitListener(metrics, threadId, offsetRecorder),
              new StoreCommitListener(storeRegistry, offsetRecorder),
              endOffsetsPoller.addForThread(threadId)
          )
      );
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
    final OffsetRecorder offsetRecorder;
    final MetricPublishingCommitListener committedOffsetMetricListener;
    final StoreCommitListener storeCommitListener;
    final EndOffsetsPoller.Listener endOffsetsPollerListener;

    public ListenersForThread(
        final String threadId,
        final OffsetRecorder offsetRecorder,
        final MetricPublishingCommitListener committedOffsetMetricListener,
        final StoreCommitListener storeCommitListener,
        final EndOffsetsPoller.Listener endOffsetsPollerListener) {
      this.threadId = threadId;
      this.offsetRecorder = offsetRecorder;
      this.committedOffsetMetricListener = committedOffsetMetricListener;
      this.storeCommitListener = storeCommitListener;
      this.endOffsetsPollerListener = endOffsetsPollerListener;
    }

    public void close() {
      committedOffsetMetricListener.close();
      endOffsetsPollerListener.close();
    }
  }

  private static class ReferenceCounted<T extends Closeable> {
    final T val;
    final String name;
    int count;

    private ReferenceCounted(final String name, final T val) {
      this.name = name;
      this.val = val;
      this.count = 1;
    }

    private void ref() {
      LOG.info("bumping ref count for {} to {}", name, count + 1);
      count += 1;
    }

    private boolean deref() {
      LOG.info("reducing ref count for {} to {}", name, count - 1);
      count -= 1;
      if (count == 0) {
        LOG.info("closing referred value for {}", name);
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

    default OffsetRecorder createOffsetRecorder(boolean eos) {
      return new OffsetRecorder(eos);
    }

    default MetricPublishingCommitListener createMetricsPublishingCommitListener(
        final Metrics metrics,
        final String threadId,
        final OffsetRecorder offsetRecorder
    ) {
      return new MetricPublishingCommitListener(metrics, threadId, offsetRecorder);
    }
  }
}
