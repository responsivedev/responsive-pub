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

package dev.responsive.kafka.api;

import static dev.responsive.kafka.api.config.ResponsiveConfig.CLIENT_ID_CONFIG;
import static dev.responsive.kafka.api.config.ResponsiveConfig.CLIENT_SECRET_CONFIG;
import static dev.responsive.kafka.api.config.ResponsiveConfig.STORAGE_BACKEND_TYPE_CONFIG;
import static dev.responsive.kafka.api.config.ResponsiveConfig.STORAGE_HOSTNAME_CONFIG;
import static dev.responsive.kafka.api.config.ResponsiveConfig.TASK_ASSIGNOR_CLASS_OVERRIDE;
import static dev.responsive.kafka.internal.metrics.ResponsiveMetrics.RESPONSIVE_METRICS_NAMESPACE;

import dev.responsive.kafka.api.config.ResponsiveConfig;
import dev.responsive.kafka.api.config.StorageBackend;
import dev.responsive.kafka.internal.clients.ResponsiveKafkaClientSupplier;
import dev.responsive.kafka.internal.config.InternalSessionConfigs;
import dev.responsive.kafka.internal.config.ResponsiveStreamsConfig;
import dev.responsive.kafka.internal.db.CassandraClientFactory;
import dev.responsive.kafka.internal.db.DefaultCassandraClientFactory;
import dev.responsive.kafka.internal.db.mongo.ResponsiveMongoClient;
import dev.responsive.kafka.internal.metrics.ClientVersionMetadata;
import dev.responsive.kafka.internal.metrics.ResponsiveMetrics;
import dev.responsive.kafka.internal.metrics.ResponsiveStateListener;
import dev.responsive.kafka.internal.stores.ResponsiveRestoreListener;
import dev.responsive.kafka.internal.stores.ResponsiveStoreRegistry;
import dev.responsive.kafka.internal.utils.SessionUtil;
import dev.responsive.kafka.internal.utils.SessionClients;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsConfig.InternalConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResponsiveKafkaStreams extends KafkaStreams {

  private static final Logger LOG = LoggerFactory.getLogger(ResponsiveKafkaStreams.class);

  private final ResponsiveMetrics responsiveMetrics;
  private final ResponsiveStateListener responsiveStateListener;
  private final ResponsiveRestoreListener responsiveRestoreListener;
  private final SessionClients sessionClients;

  /**
   * Create a {@code ResponsiveKafkaStreams} instance.
   * <p>
   * Should be used in exactly the same way as the regular {@link KafkaStreams}.
   * <p>
   * Note: even if you never call {@link #start()} on a {@code ResponsiveKafkaStreams} instance,
   * you still must {@link #close()} it to avoid resource leaks.
   *
   * @param topology       the topology specifying the computational logic
   * @param configs        map with all {@link ResponsiveConfig} and {@link StreamsConfig} props
   * @throws StreamsException if any fatal error occurs
   */
  public ResponsiveKafkaStreams(
      final Topology topology,
      final Map<?, ?> configs
  ) {
    this(topology, configs, Time.SYSTEM);
  }

  /**
   * Create a {@code ResponsiveKafkaStreams} instance.
   * <p>
   * Should be used in exactly the same way as the regular {@link KafkaStreams}.
   * <p>
   * Note: even if you never call {@link #start()} on a {@code ResponsiveKafkaStreams} instance,
   * you still must {@link #close()} it to avoid resource leaks.
   *
   * @param topology       the topology specifying the computational logic
   * @param configs        map with all {@link ResponsiveConfig} and {@link StreamsConfig} props
   * @param clientSupplier the Kafka clients supplier which provides underlying admin, producer,
   *                       and main/restore/global consumer clients
   * @throws StreamsException if any fatal error occurs
   */
  public ResponsiveKafkaStreams(
      final Topology topology,
      final Map<?, ?> configs,
      final KafkaClientSupplier clientSupplier
  ) {
    this(topology, configs, clientSupplier, Time.SYSTEM);
  }

  /**
   * Create a {@code ResponsiveKafkaStreams} instance.
   * <p>
   * Should be used in exactly the same way as the regular {@link KafkaStreams}.
   * <p>
   * Note: even if you never call {@link #start()} on a {@code ResponsiveKafkaStreams} instance,
   * you still must {@link #close()} it to avoid resource leaks.
   *
   * @param topology       the topology specifying the computational logic
   * @param configs        map with all {@link ResponsiveConfig} and {@link StreamsConfig} props
   * @param time           {@code Time} implementation; cannot be null
   * @throws StreamsException if any fatal error occurs
   */
  public ResponsiveKafkaStreams(
      final Topology topology,
      final Map<?, ?> configs,
      final Time time
  ) {
    this(topology, configs, new DefaultKafkaClientSupplier(), time);
  }

  /**
   * Create a {@code ResponsiveKafkaStreams} instance.
   * <p>
   * Should be used in exactly the same way as the regular {@link KafkaStreams}.
   * <p>
   * Note: even if you never call {@link #start()} on a {@code ResponsiveKafkaStreams} instance,
   * you still must {@link #close()} it to avoid resource leaks.
   *
   * @param topology       the topology specifying the computational logic
   * @param configs        map with all {@link ResponsiveConfig} and {@link StreamsConfig} props
   * @param clientSupplier the Kafka clients supplier which provides underlying admin, producer,
   *                       and main/restore/global consumer clients
   * @param time           {@code Time} implementation; cannot be null
   * @throws StreamsException if any fatal error occurs
   */
  public ResponsiveKafkaStreams(
      final Topology topology,
      final Map<?, ?> configs,
      final KafkaClientSupplier clientSupplier,
      final Time time
  ) {
    this(
        new Params(topology, configs)
            .withClientSupplier(clientSupplier)
            .withTime(time)
            .build()
    );
  }

  protected ResponsiveKafkaStreams(final Params params) {
    super(
        params.topology,
        propsWithOverrides(
            params.responsiveConfig.originals(),
            params.sessionClients,
            params.storeRegistry,
            params.topology.describe()),
        params.responsiveKafkaClientSupplier,
        params.time
    );

    try {
      ResponsiveStreamsConfig.validateStreamsConfig(applicationConfigs);
    } catch (final ConfigException e) {
      throw new StreamsException("Configuration error, please check your properties");
    }

    this.responsiveMetrics = params.metrics;
    this.sessionClients = params.sessionClients;

    final ClientVersionMetadata versionMetadata = ClientVersionMetadata.loadVersionMetadata();
    // Only log the version metadata for Responsive since Kafka Streams will log its own
    LOG.info("Responsive Client version: {}", versionMetadata.responsiveClientVersion);
    LOG.info("Responsive Client commit ID: {}", versionMetadata.responsiveClientCommitId);


    responsiveMetrics.initializeTags(
        applicationConfigs.getString(StreamsConfig.APPLICATION_ID_CONFIG),
        clientId,
        versionMetadata,
        applicationConfigs.originalsWithPrefix(CommonClientConfigs.METRICS_CONTEXT_PREFIX)
    );

    responsiveRestoreListener = new ResponsiveRestoreListener(responsiveMetrics);
    responsiveStateListener = new ResponsiveStateListener(responsiveMetrics);

    super.setGlobalStateRestoreListener(responsiveRestoreListener);
    super.setStateListener(responsiveStateListener);
  }

  private static ResponsiveMetrics createMetrics(final StreamsConfig config) {
    final MetricConfig metricConfig = new MetricConfig()
        .samples(config.getInt(StreamsConfig.METRICS_NUM_SAMPLES_CONFIG))
        .recordLevel(Sensor.RecordingLevel.forName(
            config.getString(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG)))
        .timeWindow(
            config.getLong(StreamsConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG), TimeUnit.MILLISECONDS);

    final JmxReporter jmxReporter = new JmxReporter();
    jmxReporter.configure(config.originals());
    return new ResponsiveMetrics(new Metrics(
        metricConfig,
        Collections.singletonList(jmxReporter),
        Time.SYSTEM,
        new KafkaMetricsContext(
            RESPONSIVE_METRICS_NAMESPACE,
            config.originalsWithPrefix(CommonClientConfigs.METRICS_CONTEXT_PREFIX))
    ));
  }

  /**
   * Fill in the props with any overrides and all internal objects shared via the configs
   * before these get finalized as a {@link StreamsConfig} object
   */
  private static Properties propsWithOverrides(
      final Map<?, ?> configs,
      final SessionClients sessionClients,
      final ResponsiveStoreRegistry storeRegistry,
      final TopologyDescription topologyDescription
  ) {
    final Properties propsWithOverrides = new Properties();

    propsWithOverrides.putAll(configs);
    propsWithOverrides.putAll(new InternalSessionConfigs.Builder()
            .withSessionClients(sessionClients)
            .withStoreRegistry(storeRegistry)
            .withTopologyDescription(topologyDescription)
            .build());

    final Object o = configs.get(InternalConfig.INTERNAL_TASK_ASSIGNOR_CLASS);
    if (o == null) {
      propsWithOverrides.put(
          InternalConfig.INTERNAL_TASK_ASSIGNOR_CLASS,
          TASK_ASSIGNOR_CLASS_OVERRIDE
      );
    } else if (!TASK_ASSIGNOR_CLASS_OVERRIDE.equals(o.toString())) {
      final String errorMsg = String.format(
          "Invalid Streams configuration value for '%s': got %s, expected '%s'",
          InternalConfig.INTERNAL_TASK_ASSIGNOR_CLASS,
          o,
          TASK_ASSIGNOR_CLASS_OVERRIDE
      );
      LOG.error(errorMsg);
      throw new ConfigException(errorMsg);
    }

    return propsWithOverrides;
  }

  @Override
  public void setStateListener(final StateListener stateListener) {
    responsiveStateListener.registerUserStateListener(stateListener);
  }

  public StateListener stateListener() {
    return responsiveStateListener.userStateListener();
  }

  /**
   * Sets a global state restore listener -- note that this is "global" in the sense that
   * it is set at the client-level and will be invoked for all stores in this topology, not that
   * it will be invoked for global state stores. This will only be invoked for ACTIVE task types,
   * ie not for global or standby tasks.
   *
   * @param restoreListener The listener triggered when a {@link StateStore} is being restored.
   */
  @Override
  public void setGlobalStateRestoreListener(final StateRestoreListener restoreListener) {
    responsiveRestoreListener.registerUserRestoreListener(restoreListener);
  }

  public StateRestoreListener stateRestoreListener() {
    return responsiveRestoreListener.userRestoreListener();
  }

  private void closeInternal() {
    responsiveStateListener.close();
    sessionClients.closeAll();
  }

  /**
   * Get read-only handle on the complete responsive metrics registry, which contains all
   * custom Responsive metrics as well as the original Streams metrics (which in turn
   * contain the streams client's own metrics plus its embedded producer, consumer and
   * admin clients' metrics across all stream threads.
   *
   * @return a map of all metrics for this Responsive Streams client, equivalent to the
   *         combination of {@link #streamsMetrics()} and {@link #responsiveMetrics()}
   */
  @Override
  public Map<MetricName, ? extends Metric> metrics() {
    final Map<MetricName, Metric> allMetrics = new LinkedHashMap<>();
    allMetrics.putAll(streamsMetrics());
    allMetrics.putAll(responsiveMetrics());
    return Collections.unmodifiableMap(allMetrics);
  }

  public Map<MetricName, ? extends Metric> streamsMetrics() {
    return super.metrics();
  }

  public Map<MetricName, ? extends Metric> responsiveMetrics() {
    return responsiveMetrics.metrics();
  }

  @Override
  public void close() {
    super.close();
    closeInternal();
  }

  @Override
  public boolean close(final Duration timeout) {
    final boolean closed = super.close(timeout);
    closeInternal();
    return closed;
  }

  @Override
  public boolean close(final CloseOptions options) {
    final boolean closed = super.close(options);
    closeInternal();
    return closed;
  }

  protected static class Params {

    final Topology topology;
    final ResponsiveConfig responsiveConfig;
    final StreamsConfig streamsConfig;
    final ResponsiveMetrics metrics;
    final ResponsiveStoreRegistry storeRegistry;

    // can be set during construction
    private Time time = Time.SYSTEM;
    private KafkaClientSupplier clientSupplier = new DefaultKafkaClientSupplier();
    private CassandraClientFactory cassandraFactory = new DefaultCassandraClientFactory();

    // initialized on init()
    private SessionClients sessionClients;
    private ResponsiveKafkaClientSupplier responsiveKafkaClientSupplier;

    public Params(final Topology topology, final Map<?, ?> configs) {
      this.topology = topology;

      // this should be the only place this is logged!
      this.responsiveConfig = ResponsiveConfig.loggedConfig(configs);
      this.streamsConfig = ResponsiveStreamsConfig.streamsConfig(configs);

      this.metrics = createMetrics(streamsConfig);
      this.storeRegistry = new ResponsiveStoreRegistry();
    }

    public Params withClientSupplier(final KafkaClientSupplier clientSupplier) {
      this.clientSupplier = clientSupplier;
      return this;
    }

    public Params withCassandraClientFactory(final CassandraClientFactory clientFactory) {
      this.cassandraFactory = clientFactory;
      return this;
    }

    public Params withTime(final Time time) {
      this.time = time;
      return this;
    }

    // we may want to consider wrapping this in an additional Builder to ensure
    // that it's impossible to use a Params instance that hasn't called build(),
    // but that felt a little extra
    public Params build() {
      this.responsiveKafkaClientSupplier = new ResponsiveKafkaClientSupplier(
          clientSupplier,
          streamsConfig,
          storeRegistry,
          metrics
      );
      final var admin = responsiveKafkaClientSupplier.getAdmin(responsiveConfig.originals());

      final var backendType = StorageBackend.valueOf(responsiveConfig
          .getString(STORAGE_BACKEND_TYPE_CONFIG)
          .toUpperCase(Locale.ROOT)
      );

      switch (backendType) {
        case CASSANDRA:
          final var cqlSession = cassandraFactory.createCqlSession(responsiveConfig);
          final var cassandraClient = cassandraFactory.createClient(cqlSession, responsiveConfig);
          sessionClients = new SessionClients(Optional.empty(), Optional.of(cassandraClient), admin);
          break;
        case MONGO_DB:
          final var hostname = responsiveConfig.getString(STORAGE_HOSTNAME_CONFIG);
          final String clientId = responsiveConfig.getString(CLIENT_ID_CONFIG);
          final Password clientSecret = responsiveConfig.getPassword(CLIENT_SECRET_CONFIG);
          final var mongoClient = SessionUtil.connect(
              hostname,
              clientId,
              clientSecret == null ? null : clientSecret.value()
          );
          sessionClients = new SessionClients(
              Optional.of(new ResponsiveMongoClient(mongoClient)),
              Optional.empty(),
              admin
          );
          break;
        default:
          throw new IllegalStateException("Unexpected value: " + backendType);
      }

      return this;
    }
  }
}
