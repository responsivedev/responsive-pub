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

package dev.responsive.spring.config;

import dev.responsive.kafka.api.ResponsiveKafkaStreams;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyConfig;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;
import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.KafkaStreamsCustomizer;
import org.springframework.kafka.config.KafkaStreamsInfrastructureCustomizer;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.core.CleanupConfig;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

public class ResponsiveFactoryBean extends StreamsBuilderFactoryBean {

  public static final Duration DEFAULT_CLOSE_TIMEOUT = Duration.ofSeconds(10);

  private static final LogAccessor LOGGER =
      new LogAccessor(LogFactory.getLog(StreamsBuilderFactoryBean.class));

  private final ReentrantLock lifecycleLock = new ReentrantLock();
  private final List<StreamsBuilderFactoryBean.Listener> listeners = new ArrayList<>();

  private KafkaClientSupplier clientSupplier = new DefaultKafkaClientSupplier();
  private Properties properties;
  private CleanupConfig cleanupConfig;
  private KafkaStreamsInfrastructureCustomizer infrastructureCustomizer = new InfraCustomizer();

  private KafkaStreamsCustomizer kafkaStreamsCustomizer;
  private KafkaStreams.StateListener stateListener;
  private StateRestoreListener stateRestoreListener;
  private StreamsUncaughtExceptionHandler streamsUncaughtExceptionHandler;
  private boolean autoStartup = true;
  private int phase = Integer.MAX_VALUE - 1000; // NOSONAR magic #
  private Duration closeTimeout = DEFAULT_CLOSE_TIMEOUT;
  private boolean leaveGroupOnClose = false;
  private KafkaStreams kafkaStreams;
  private volatile boolean running;
  private Topology topology;
  private String beanName;

  public ResponsiveFactoryBean() {
    this.cleanupConfig = new CleanupConfig();
  }

  public ResponsiveFactoryBean(KafkaStreamsConfiguration streamsConfiguration) {
    this(streamsConfiguration, new CleanupConfig());
  }

  public ResponsiveFactoryBean(
      KafkaStreamsConfiguration streamsConfig,
      CleanupConfig cleanupConfig
  ) {
    super(streamsConfig, cleanupConfig);
    Assert.notNull(streamsConfig, "'streamsConfig' must not be null");
    Assert.notNull(cleanupConfig, "'cleanupConfig' must not be null");
    this.properties = streamsConfig.asProperties();
    this.cleanupConfig = cleanupConfig;
  }

  @Override
  public synchronized void setBeanName(final String name) {
    this.beanName = name;
  }

  @Override
  public void setStreamsConfiguration(Properties streamsConfig) {
    Assert.notNull(streamsConfig, "'streamsConfig' must not be null");
    this.properties = streamsConfig;
  }

  @Override
  @Nullable
  public Properties getStreamsConfiguration() {
    return properties;
  }

  @Override
  public void setClientSupplier(final KafkaClientSupplier clientSupplier) {
    Assert.notNull(clientSupplier, "'clientSupplier' must not be null");
    this.clientSupplier = clientSupplier;
  }

  @Override
  public void setInfrastructureCustomizer(
      final KafkaStreamsInfrastructureCustomizer infrastructureCustomizer
  ) {
    Assert.notNull(infrastructureCustomizer, "'infrastructureCustomizer' must not be null");
    this.infrastructureCustomizer = infrastructureCustomizer;
  }

  @Override
  public void setKafkaStreamsCustomizer(final KafkaStreamsCustomizer kafkaStreamsCustomizer) {
    Assert.notNull(kafkaStreamsCustomizer, "'kafkaStreamsCustomizer' must not be null");
    this.kafkaStreamsCustomizer = kafkaStreamsCustomizer;
  }

  @Override
  public void setStateListener(final KafkaStreams.StateListener stateListener) {
    this.stateListener = stateListener;
  }

  @Override
  public void setStreamsUncaughtExceptionHandler(
      final StreamsUncaughtExceptionHandler streamsUncaughtExceptionHandler
  ) {
    this.streamsUncaughtExceptionHandler = streamsUncaughtExceptionHandler;
  }

  @Override
  public StreamsUncaughtExceptionHandler getStreamsUncaughtExceptionHandler() {
    return streamsUncaughtExceptionHandler;
  }

  @Override
  public void setStateRestoreListener(final StateRestoreListener stateRestoreListener) {
    this.stateRestoreListener = stateRestoreListener;
  }

  @Override
  public void setCloseTimeout(final int closeTimeout) {
    this.closeTimeout = Duration.ofSeconds(closeTimeout);
  }

  @Override
  public void setLeaveGroupOnClose(final boolean leaveGroupOnClose) {
    this.leaveGroupOnClose = leaveGroupOnClose;
  }

  @Override
  public Topology getTopology() {
    return topology;
  }

  @Override
  public Class<?> getObjectType() {
    return super.getObjectType();
  }

  @Override
  public void setAutoStartup(final boolean autoStartup) {
    this.autoStartup = autoStartup;
  }

  @Override
  public void setPhase(final int phase) {
    this.phase = phase;
  }

  @Override
  public int getPhase() {
    return this.phase;
  }

  @Override
  public void setCleanupConfig(final CleanupConfig cleanupConfig) {
    this.cleanupConfig = cleanupConfig;
  }

  @Override
  @Nullable
  public synchronized KafkaStreams getKafkaStreams() {
    this.lifecycleLock.lock();
    try {
      return this.kafkaStreams;
    } finally {
      this.lifecycleLock.unlock();
    }
  }

  @Override
  public List<Listener> getListeners() {
    return Collections.unmodifiableList(this.listeners);
  }

  @Override
  public void addListener(Listener listener) {
    Assert.notNull(listener, "'listener' cannot be null");
    this.listeners.add(listener);
  }

  @Override
  public boolean removeListener(Listener listener) {
    return this.listeners.remove(listener);
  }

  @Override
  protected StreamsBuilder createInstance() {
    this.lifecycleLock.lock();
    try {
      if (this.autoStartup) {
        Assert.state(
            this.properties != null,
            "streams configuration properties must not be null"
        );
      }
      StreamsBuilder builder = createStreamBuilder();
      this.infrastructureCustomizer.configureBuilder(builder);
      return builder;
    } finally {
      this.lifecycleLock.unlock();
    }
  }

  @Override
  public boolean isAutoStartup() {
    return this.autoStartup;
  }

  @Override
  public void start() {
    this.lifecycleLock.lock();
    try {
      if (!this.running) {
        try {
          Assert.state(
              this.properties != null,
              "streams configuration properties must not be null"
          );
          this.kafkaStreams = new ResponsiveKafkaStreams(
              this.topology,
              this.properties,
              this.clientSupplier
          );
          this.kafkaStreams.setStateListener(this.stateListener);
          this.kafkaStreams.setGlobalStateRestoreListener(this.stateRestoreListener);
          if (this.streamsUncaughtExceptionHandler != null) {
            this.kafkaStreams.setUncaughtExceptionHandler(this.streamsUncaughtExceptionHandler);
          }
          if (this.kafkaStreamsCustomizer != null) {
            this.kafkaStreamsCustomizer.customize(this.kafkaStreams);
          }
          if (this.cleanupConfig.cleanupOnStart()) {
            this.kafkaStreams.cleanUp();
          }
          this.kafkaStreams.start();
          for (Listener listener : this.listeners) {
            listener.streamsAdded(this.beanName, this.kafkaStreams);
          }
          this.running = true;
        } catch (Exception e) {
          throw new KafkaException("Could not start stream: ", e);
        }
      }
    } finally {
      this.lifecycleLock.unlock();
    }
  }

  @Override
  public void stop(Runnable callback) {
    stop();
    if (callback != null) {
      callback.run();
    }
  }

  @Override
  public void stop() {
    this.lifecycleLock.lock();
    try {
      if (this.running) {
        try {
          if (this.kafkaStreams != null) {
            this.kafkaStreams.close(new KafkaStreams.CloseOptions()
                .timeout(this.closeTimeout)
                .leaveGroup(this.leaveGroupOnClose)
            );
            if (this.cleanupConfig.cleanupOnStop()) {
              this.kafkaStreams.cleanUp();
            }
            for (Listener listener : this.listeners) {
              listener.streamsRemoved(this.beanName, this.kafkaStreams);
            }
            this.kafkaStreams = null;
          }
        } catch (Exception e) {
          LOGGER.error(e, "Failed to stop streams");
        } finally {
          this.running = false;
        }
      }
    } finally {
      this.lifecycleLock.unlock();
    }
  }

  @Override
  public void afterSingletonsInstantiated() {
    try {
      this.topology = getObject().build(this.properties);
      this.infrastructureCustomizer.configureTopology(this.topology);
      LOGGER.debug(() -> this.topology.describe().toString());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean isRunning() {
    this.lifecycleLock.lock();
    try {
      return this.running;
    } finally {
      this.lifecycleLock.unlock();
    }
  }

  private StreamsBuilder createStreamBuilder() {
    if (this.properties == null) {
      return new StreamsBuilder();
    } else {
      StreamsConfig streamsConfig = new StreamsConfig(this.properties);
      TopologyConfig topologyConfig = new TopologyConfig(streamsConfig);
      return new StreamsBuilder(topologyConfig);
    }
  }

  private static class InfraCustomizer implements KafkaStreamsInfrastructureCustomizer {
  }
}
