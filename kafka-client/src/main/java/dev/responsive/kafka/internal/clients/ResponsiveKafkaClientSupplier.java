/*
 * Copyright 2024 Responsive Computing, Inc.
 *
 * This source code is licensed under the Responsive Business Source License Agreement v1.0
 * available at:
 *
 * https://www.responsive.dev/legal/responsive-bsl-10
 *
 * This software requires a valid Commercial License Key for production use. Trial and commercial
 * licenses can be obtained at https://www.responsive.dev
 */

package dev.responsive.kafka.internal.clients;

import static dev.responsive.kafka.internal.config.ConfigUtils.eosEnabled;
import static dev.responsive.kafka.internal.utils.Utils.extractThreadIdFromConsumerClientId;
import static dev.responsive.kafka.internal.utils.Utils.extractThreadIdFromProducerClientId;
import static dev.responsive.kafka.internal.utils.Utils.extractThreadIdFromRestoreConsumerClientId;

import dev.responsive.kafka.api.config.ResponsiveConfig;
import dev.responsive.kafka.api.config.StorageBackend;
import dev.responsive.kafka.internal.license.LicenseUtils;
import dev.responsive.kafka.internal.license.model.CloudLicenseV1;
import dev.responsive.kafka.internal.license.model.TimedTrialV1;
import dev.responsive.kafka.internal.license.model.UsageBasedV1;
import dev.responsive.kafka.internal.metrics.EndOffsetsPoller;
import dev.responsive.kafka.internal.metrics.MetricPublishingCommitListener;
import dev.responsive.kafka.internal.metrics.ResponsiveMetrics;
import dev.responsive.kafka.internal.stores.ResponsiveStoreRegistry;
import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.function.Function;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
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
  private final ResponsiveMetrics metrics;
  private final EndOffsetsPoller endOffsetsPoller;
  private final String applicationId;
  private final boolean eos;
  private final StorageBackend storageBackend;
  private final boolean repairRestoreOffsetOutOfRange;

  public ResponsiveKafkaClientSupplier(
      final KafkaClientSupplier clientSupplier,
      final ResponsiveConfig responsiveConfig,
      final StreamsConfig configs,
      final ResponsiveStoreRegistry storeRegistry,
      final ResponsiveMetrics metrics,
      final StorageBackend storageBackend
  ) {
    this(
        configuredFactories(responsiveConfig, configs),
        clientSupplier,
        configs,
        storeRegistry,
        metrics,
        storageBackend,
        responsiveConfig.getBoolean(ResponsiveConfig.RESTORE_OFFSET_REPAIR_ENABLED_CONFIG)
    );
  }

  ResponsiveKafkaClientSupplier(
      final Factories factories,
      final KafkaClientSupplier wrapped,
      final StreamsConfig configs,
      final ResponsiveStoreRegistry storeRegistry,
      final ResponsiveMetrics metrics,
      final StorageBackend storageBackend,
      final boolean repairRestoreOffsetOutOfRange
  ) {
    this.factories = factories;
    this.wrapped = wrapped;
    this.storeRegistry = storeRegistry;
    this.metrics = metrics;
    this.storageBackend = storageBackend;
    this.repairRestoreOffsetOutOfRange = repairRestoreOffsetOutOfRange;

    eos = eosEnabled(configs);

    endOffsetsPoller = factories.createEndOffsetPoller(
        configs.originals(),
        metrics,
        this
    );
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
    final String threadId = extractThreadIdFromProducerClientId(clientId);
    final ListenersForThread tc = sharedListeners.getAndMaybeInitListenersForThread(
        eos,
        threadId,
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
                tc.originEventRecorder,
                new CloseListener(threadId)
            )
        )
    );
  }

  @Override
  public Consumer<byte[], byte[]> getConsumer(final Map<String, Object> config) {
    final String clientId = (String) config.get(ConsumerConfig.CLIENT_ID_CONFIG);
    LOG.info("Creating responsive main consumer: {}", clientId);

    final String threadId = extractThreadIdFromConsumerClientId(clientId);

    final ListenersForThread tc = sharedListeners.getAndMaybeInitListenersForThread(
        eos,
        threadId,
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
            tc.originEventRecorder,
            new CloseListener(threadId)
        )
    );
  }

  @Override
  public Consumer<byte[], byte[]> getRestoreConsumer(final Map<String, Object> config) {
    if (storageBackend == StorageBackend.NONE) {
      return wrapped.getRestoreConsumer(config);
    }

    final String clientId = (String) config.get(ConsumerConfig.CLIENT_ID_CONFIG);
    LOG.info("Creating responsive restore consumer: {}", clientId);
    final String threadId = extractThreadIdFromRestoreConsumerClientId(clientId);
    return factories.createRestoreConsumer(
        clientId,
        wrapped.getRestoreConsumer(config),
        p -> storeRegistry.getCommittedOffset(p, threadId),
        repairRestoreOffsetOutOfRange
    );

  }

  @Override
  public Consumer<byte[], byte[]> getGlobalConsumer(final Map<String, Object> config) {
    if (storageBackend == StorageBackend.NONE) {
      return wrapped.getGlobalConsumer(config);
    }

    LOG.info("Creating responsive global consumer");

    config.put(ConsumerConfig.GROUP_ID_CONFIG, applicationId + "-global");
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);

    return factories.createGlobalConsumer(
        config,
        wrapped,
        getAdmin(config)
    );
  }

  class CloseListener implements ResponsiveConsumer.Listener, ResponsiveProducer.Listener {
    private final String threadId;

    private CloseListener(final String threadId) {
      this.threadId = threadId;
    }

    @Override
    public void onProducerClose() {
      sharedListeners.derefListenersForThread(threadId);
    }

    @Override
    public void onConsumerClose() {
      sharedListeners.derefListenersForThread(threadId);
    }
  }

  private static class SharedListeners {
    private final Map<String, ReferenceCounted<ListenersForThread>> threadListeners
        = new HashMap<>();

    private synchronized ListenersForThread getAndMaybeInitListenersForThread(
        final boolean eos,
        final String threadId,
        final ResponsiveMetrics metrics,
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
      final var offsetRecorder = factories.createOffsetRecorder(eos, threadId);
      final var originEventRecorder = factories.createOriginEventRecorder(threadId);
      final var tl = new ReferenceCounted<>(
          String.format("ListenersForThread(%s)", threadId),
          new ListenersForThread(
              threadId,
              offsetRecorder,
              factories.createMetricsPublishingCommitListener(
                  metrics,
                  threadId,
                  offsetRecorder
              ),
              new StoreCommitListener(storeRegistry, offsetRecorder),
              endOffsetsPoller.addForThread(threadId),
              originEventRecorder
          )
      );
      threadListeners.put(threadId, tl);
      return tl.getVal();
    }

    private synchronized void derefListenersForThread(final String threadId) {
      final var listener = threadListeners.get(threadId);
      if (listener == null) {
        final String errorMsg = String.format("Could not find thread listener for thread id %s. "
                                                  + "Current thread name: %s. Other registered "
                                                  + "thread listener ids: %s",
                                              threadId, Thread.currentThread().getName(),
                                              threadListeners.keySet());
        LOG.error(errorMsg);
        throw new IllegalStateException(errorMsg);
      }

      if (listener.deref()) {
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
    final OriginEventRecorder originEventRecorder;

    public ListenersForThread(
        final String threadId,
        final OffsetRecorder offsetRecorder,
        final MetricPublishingCommitListener committedOffsetMetricListener,
        final StoreCommitListener storeCommitListener,
        final EndOffsetsPoller.Listener endOffsetsPollerListener,
        final OriginEventRecorder originEventRecorder
    ) {
      this.threadId = threadId;
      this.offsetRecorder = offsetRecorder;
      this.committedOffsetMetricListener = committedOffsetMetricListener;
      this.storeCommitListener = storeCommitListener;
      this.endOffsetsPollerListener = endOffsetsPollerListener;
      this.originEventRecorder = originEventRecorder;
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

  private static Factories configuredFactories(
      final ResponsiveConfig responsiveConfig,
      final StreamsConfig streamsConfig
  ) {
    return threadId -> {
      // we should consider caching the license in ResponsiveConfig so that we
      // don't have to load and authenticate it more than once
      final var license = LicenseUtils.loadLicense(responsiveConfig);
      switch (license.type()) {
        case CloudLicenseV1.TYPE_NAME:
        case TimedTrialV1.TYPE_NAME:
          // we don't report origin events for timed trial
          // or cloud licenses since we don't want to modify
          // the headers of records if not necessary
          return new NoopOriginEventRecorder();
        case UsageBasedV1.TYPE_NAME:
        default:
          return new OriginEventRecorderImpl(
              threadId,
              responsiveConfig,
              license,
              eosEnabled(streamsConfig)
          );
      }
    };
  }

  interface Factories {
    default EndOffsetsPoller createEndOffsetPoller(
        final Map<String, ?> config,
        final ResponsiveMetrics metrics,
        final KafkaClientSupplier clientSupplier
    ) {
      return new EndOffsetsPoller(config, metrics, clientSupplier::getAdmin);
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
        final List<ResponsiveConsumer.Listener> listeners
    ) {
      return new ResponsiveConsumer<>(clientId, wrapped, listeners);
    }

    default <K, V> ResponsiveGlobalConsumer createGlobalConsumer(
        final Map<String, Object> config,
        final KafkaClientSupplier wrapped,
        final Admin admin
    ) {
      return new ResponsiveGlobalConsumer(
          config,
          wrapped.getGlobalConsumer(config),
          admin
      );
    }

    default OffsetRecorder createOffsetRecorder(boolean eos, final String threadId) {
      return new OffsetRecorder(eos, threadId);
    }

    default MetricPublishingCommitListener createMetricsPublishingCommitListener(
        final ResponsiveMetrics metrics,
        final String threadId,
        final OffsetRecorder offsetRecorder
    ) {
      return new MetricPublishingCommitListener(
          metrics,
          threadId,
          offsetRecorder
      );
    }

    default ResponsiveRestoreConsumer<byte[], byte[]> createRestoreConsumer(
        final String clientId,
        final Consumer<byte[], byte[]> restoreConsumer,
        final Function<TopicPartition, OptionalLong> getCommittedOffset,
        final boolean repairRestoreOffsetOutOfRange
    ) {
      return new ResponsiveRestoreConsumer<>(
          clientId,
          restoreConsumer,
          getCommittedOffset,
          repairRestoreOffsetOutOfRange
      );
    }

    OriginEventRecorder createOriginEventRecorder(final String threadId);
  }
}
