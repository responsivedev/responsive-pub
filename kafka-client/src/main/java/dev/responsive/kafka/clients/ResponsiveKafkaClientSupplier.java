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

import dev.responsive.kafka.store.ResponsiveStoreRegistry;
import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.StreamsConfig;
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
  private final Metrics metrics;
  private final EndOffsetsPoller endOffsetsPoller;
  private final String applicationId;
  private final boolean eos;

  public ResponsiveKafkaClientSupplier(
      final KafkaClientSupplier clientSupplier,
      final StreamsConfig configs,
      final ResponsiveStoreRegistry storeRegistry,
      final Metrics metrics
  ) {
    this(new Factories() {}, clientSupplier, configs, storeRegistry, metrics);
  }

  ResponsiveKafkaClientSupplier(
      final Factories factories,
      final KafkaClientSupplier wrapped,
      final StreamsConfig configs,
      final ResponsiveStoreRegistry storeRegistry,
      final Metrics metrics
  ) {
    this.factories = factories;
    this.wrapped = wrapped;
    this.storeRegistry = storeRegistry;
    this.metrics = metrics;

    eos = !(AT_LEAST_ONCE.equals(
        configs.getString(StreamsConfig.PROCESSING_GUARANTEE_CONFIG)));

    endOffsetsPoller = factories.createEndOffsetPoller(configs.originals(), metrics);
    applicationId = configs.getString(StreamsConfig.APPLICATION_ID_CONFIG);
  }

  @Override
  public Admin getAdmin(final Map<String, Object> config) {
    return wrapped.getAdmin(config);
  }

  @Override
  public Producer<byte[], byte[]> getProducer(final Map<String, Object> config) {
    final String clientId = (String) config.get(ProducerConfig.CLIENT_ID_CONFIG);
    LOG.info("Creating responsive producer: {}", clientId);
    final String tid = threadIdFromProducerConfig(clientId);
    final ListenersForThread tc = sharedListeners.getAndMaybeInitListenersForThread(
        eos,
        tid,
        metrics,
        applicationId,
        config,
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
    final String clientId = (String) config.get(ConsumerConfig.CLIENT_ID_CONFIG);
    LOG.info("Creating responsive main consumer: {}", clientId);
    final String tid = threadIdFromConsumerConfig(clientId);
    final ListenersForThread tc = sharedListeners.getAndMaybeInitListenersForThread(
        eos,
        tid,
        metrics,
        applicationId,
        config,
        endOffsetsPoller,
        storeRegistry,
        factories
    );
    // TODO: the end offsets poller call is kind of heavy for a synchronized block
    return factories.createResponsiveConsumer(
        clientId,
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
    final String clientId = (String) config.get(ConsumerConfig.CLIENT_ID_CONFIG);
    LOG.info("Creating responsive restore consumer: {}", clientId);
    return new ResponsiveRestoreConsumer<>(
        clientId,
        wrapped.getRestoreConsumer(config),
        storeRegistry::getCommittedOffset
    );
  }

  @Override
  public Consumer<byte[], byte[]> getGlobalConsumer(final Map<String, Object> config) {
    LOG.info("Creating responsive global consumer");

    config.put(ConsumerConfig.GROUP_ID_CONFIG, applicationId + "-global");
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);

    return new ResponsiveGlobalConsumer(
        config,
        wrapped.getGlobalConsumer(config),
        getAdmin(config)
    );
  }

  private String threadIdFromProducerConfig(final String clientId) {
    final var regex = Pattern.compile(".*-(StreamThread-\\d+)-producer");
    final var match = regex.matcher(clientId);
    if (!match.find()) {
      LOG.error("Unable to parse thread id from producer client id = {}", clientId);
      throw new RuntimeException("unexpected client id " + clientId);
    }
    return match.group(1);
  }

  private String threadIdFromConsumerConfig(final String clientId) {
    final var regex = Pattern.compile(".*-(StreamThread-\\d+)-consumer$");
    final var match = regex.matcher(clientId);
    if (!match.find()) {
      LOG.error("Unable to parse thread id from consumer client id = {}", clientId);
      throw new RuntimeException("Unexpected client id " + clientId);
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
        final String consumerGroup,
        final Map<String, Object> configs,
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
              factories.createMetricsPublishingCommitListener(
                  metrics,
                  threadId,
                  consumerGroup,
                  offsetRecorder
              ),
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
        final EndOffsetsPoller.Listener endOffsetsPollerListener
    ) {
      this.threadId = threadId;
      this.offsetRecorder = offsetRecorder;
      this.committedOffsetMetricListener = committedOffsetMetricListener;
      this.storeCommitListener = storeCommitListener;
      this.endOffsetsPollerListener = endOffsetsPollerListener;
    }

    @Override
    public void close() {
      committedOffsetMetricListener.close();
      endOffsetsPollerListener.close();
    }
  }

  private static class ReferenceCounted<T extends Closeable> {
    final T val;
    final Logger refLog;
    int count;

    private ReferenceCounted(final String name, final T val) {
      this.refLog = new LogContext(String.format("[%s]: ", name)).logger(ReferenceCounted.class);
      this.val = val;
      this.count = 1;
    }

    private void ref() {
      refLog.info("Bumping ref count to {}", count + 1);
      count += 1;
    }

    private boolean deref() {
      refLog.info("Reducing ref count to {}", count - 1);
      count -= 1;
      if (count == 0) {
        refLog.info("Closing reference value");
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
        final String consumerGroup,
        final OffsetRecorder offsetRecorder
    ) {
      return new MetricPublishingCommitListener(
          metrics,
          threadId,
          consumerGroup,
          offsetRecorder
      );
    }
  }
}
