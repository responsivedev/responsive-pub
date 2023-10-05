package dev.responsive.kafka.api;

import static dev.responsive.kafka.api.config.ResponsiveConfig.TASK_ASSIGNOR_CLASS_OVERRIDE;
import static dev.responsive.kafka.internal.metrics.ResponsiveMetrics.RESPONSIVE_METRICS_NAMESPACE;

import com.datastax.oss.driver.api.core.CqlSession;
import dev.responsive.kafka.api.config.ResponsiveConfig;
import dev.responsive.kafka.internal.clients.ResponsiveKafkaClientSupplier;
import dev.responsive.kafka.internal.config.InternalConfigs;
import dev.responsive.kafka.internal.config.ResponsiveStreamsConfig;
import dev.responsive.kafka.internal.db.CassandraClient;
import dev.responsive.kafka.internal.db.CassandraClientFactory;
import dev.responsive.kafka.internal.db.DefaultCassandraClientFactory;
import dev.responsive.kafka.internal.metrics.ClientVersionMetadata;
import dev.responsive.kafka.internal.metrics.ResponsiveMetrics;
import dev.responsive.kafka.internal.metrics.ResponsiveStateListener;
import dev.responsive.kafka.internal.stores.ResponsiveRestoreListener;
import dev.responsive.kafka.internal.stores.ResponsiveStoreRegistry;
import dev.responsive.kafka.internal.utils.SharedClients;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.config.ConfigException;
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
  private final SharedClients sharedClients;

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
        topology,
        ResponsiveConfig.loggedConfig(configs), // this should be the only place this is logged!
        ResponsiveStreamsConfig.streamsConfig(configs),
        clientSupplier,
        time,
        new DefaultCassandraClientFactory()
    );
  }

  protected ResponsiveKafkaStreams(
      final Topology topology,
      final ResponsiveConfig responsiveConfig,
      final StreamsConfig baseStreamsConfig,
      final KafkaClientSupplier clientSupplier,
      final Time time,
      final CassandraClientFactory clientFactory
  ) {
    this(
        topology,
        responsiveConfig,
        baseStreamsConfig,
        clientSupplier,
        time,
        new ResponsiveStoreRegistry(),
        createMetrics(baseStreamsConfig),
        clientFactory,
        clientFactory.createCqlSession(responsiveConfig)
    );
  }

  private ResponsiveKafkaStreams(
      final Topology topology,
      final ResponsiveConfig responsiveConfig,
      final StreamsConfig baseStreamsConfig,
      final KafkaClientSupplier clientSupplier,
      final Time time,
      final ResponsiveStoreRegistry storeRegistry,
      final ResponsiveMetrics metrics,
      final CassandraClientFactory clientFactory,
      final CqlSession session
  ) {
    this(
        topology,
        responsiveConfig,
        new ResponsiveKafkaClientSupplier(
            clientSupplier,
            baseStreamsConfig,
            storeRegistry,
            metrics),
        time,
        storeRegistry,
        metrics,
        clientFactory.createCassandraClient(session, responsiveConfig)
    );
  }

  private ResponsiveKafkaStreams(
      final Topology topology,
      final ResponsiveConfig responsiveConfig,
      final ResponsiveKafkaClientSupplier responsiveKafkaClientSupplier,
      final Time time,
      final ResponsiveStoreRegistry storeRegistry,
      final ResponsiveMetrics metrics,
      final CassandraClient cassandraClient
  ) {
    this(
        topology,
        responsiveConfig,
        responsiveKafkaClientSupplier,
        time,
        storeRegistry,
        metrics,
        new SharedClients(
            cassandraClient,
            responsiveKafkaClientSupplier.getAdmin(responsiveConfig.originals())
        )
    );
  }

  private ResponsiveKafkaStreams(
      final Topology topology,
      final ResponsiveConfig responsiveConfig,
      final ResponsiveKafkaClientSupplier clientSupplier,
      final Time time,
      final ResponsiveStoreRegistry storeRegistry,
      final ResponsiveMetrics responsiveMetrics,
      final SharedClients sharedClients
  ) {
    super(
        topology,
        propsWithOverrides(
            responsiveConfig.originals(),
            sharedClients,
            storeRegistry,
            topology.describe()),
        clientSupplier,
        time
    );

    try {
      ResponsiveStreamsConfig.validateStreamsConfig(applicationConfigs);
    } catch (final ConfigException e) {
      throw new StreamsException("Configuration error, please check your properties");
    }

    this.responsiveMetrics = responsiveMetrics;
    this.sharedClients = sharedClients;

    final ClientVersionMetadata versionMetadata = ClientVersionMetadata.loadVersionMetadata();
    // Only log the version metadata for Responsive since Kafka Streams will log its own
    LOG.info("Responsive Client version: {}", versionMetadata.responsiveClientVersion);
    LOG.info("Responsive Client commit ID: {}", versionMetadata.responsiveClientCommitId);


    responsiveMetrics.initialize(
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
      final SharedClients sharedClients,
      final ResponsiveStoreRegistry storeRegistry,
      final TopologyDescription topologyDescription
  ) {
    final Properties propsWithOverrides = new Properties();
    propsWithOverrides.putAll(configs);
    propsWithOverrides.putAll(new InternalConfigs.Builder()
            .withCassandraClient(sharedClients.cassandraClient)
            .withKafkaAdmin(sharedClients.admin)
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
    sharedClients.closeAll();
  }

  /**
   * Get read-only handle on the complete responsive metrics registry, which contains all
   * custom Responsive metrics as well as the original Streams metrics (which in turn
   * contain the streams client's own metrics plus its embedded producer, consumer and
   * admin clients' metrics across all stream threads.
   * <p>
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
}
