package dev.responsive.kafka.api;

import static dev.responsive.kafka.config.ResponsiveConfig.NUM_STANDBYS_OVERRIDE;
import static dev.responsive.kafka.config.ResponsiveConfig.TASK_ASSIGNOR_CLASS_OVERRIDE;
import static org.apache.kafka.streams.StreamsConfig.EXACTLY_ONCE;

import com.datastax.oss.driver.api.core.CqlSession;
import dev.responsive.db.CassandraClient;
import dev.responsive.kafka.clients.ResponsiveKafkaClientSupplier;
import dev.responsive.kafka.clients.SharedClients;
import dev.responsive.kafka.config.ResponsiveConfig;
import dev.responsive.kafka.store.ResponsiveRestoreListener;
import dev.responsive.kafka.store.ResponsiveStoreRegistry;
import dev.responsive.utils.StoreUtil;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
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
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.ClientUtils;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResponsiveKafkaStreams extends KafkaStreams {

  private static final Logger LOG = LoggerFactory.getLogger(ResponsiveKafkaStreams.class);

  private StateListener stateListener;
  private final ResponsiveRestoreListener restoreListener;

  private final SharedClients sharedClients;

  public static ResponsiveKafkaStreams create(
      final Topology topology,
      final Map<?, ?> configs
  ) {
    return new ResponsiveKafkaStreams(topology, configs);
  }

  public ResponsiveKafkaStreams(
      final Topology topology,
      final Map<?, ?> configs
  ) {
    this(topology, configs, new DefaultKafkaClientSupplier());
  }

  public ResponsiveKafkaStreams(
      final Topology topology,
      final Map<?, ?> configs,
      final KafkaClientSupplier clientSupplier
  ) {
    this(
        topology,
        ResponsiveConfig.loggedConfig(configs),
        quietReadOnlystreamsConfig(configs),
        clientSupplier,
        new DefaultCassandraClientFactory()
    );
  }

  ResponsiveKafkaStreams(
      final Topology topology,
      final ResponsiveConfig responsiveConfig,
      final StreamsConfig baseStreamsConfig,
      final KafkaClientSupplier clientSupplier,
      final CassandraClientFactory clientFactory
  ) {
    this(
        topology,
        responsiveConfig,
        baseStreamsConfig,
        clientSupplier,
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
      final ResponsiveStoreRegistry storeRegistry,
      final Metrics metrics,
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
        storeRegistry,
        metrics,
        clientFactory.createCassandraClient(session, responsiveConfig)
    );
  }

  private ResponsiveKafkaStreams(
      final Topology topology,
      final ResponsiveConfig responsiveConfig,
      final ResponsiveKafkaClientSupplier responsiveKafkaClientSupplier,
      final ResponsiveStoreRegistry storeRegistry,
      final Metrics metrics,
      final CassandraClient cassandraClient
  ) {
    this(
        topology,
        responsiveConfig,
        responsiveKafkaClientSupplier,
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
      final ResponsiveStoreRegistry storeRegistry,
      final Metrics metrics,
      final SharedClients sharedClients
  ) {
    super(
        topology,
        verifiedStreamsConfigs(
            responsiveConfig.originals(),
            sharedClients,
            storeRegistry,
            topology.describe()),
        clientSupplier
    );
    this.sharedClients = sharedClients;
    this.restoreListener = new ResponsiveRestoreListener(metrics);

    super.setGlobalStateRestoreListener(restoreListener);
  }

  private static Metrics createMetrics(final StreamsConfig config) {
    final MetricConfig metricConfig = new MetricConfig()
        .samples(config.getInt(StreamsConfig.METRICS_NUM_SAMPLES_CONFIG))
        .recordLevel(Sensor.RecordingLevel.forName(
            config.getString(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG)))
        .timeWindow(
            config.getLong(StreamsConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG), TimeUnit.MILLISECONDS);

    final JmxReporter jmxReporter = new JmxReporter();
    jmxReporter.configure(config.originals());
    return new Metrics(
        metricConfig,
        Collections.singletonList(jmxReporter),
        Time.SYSTEM,
        new KafkaMetricsContext("dev.responsive", new HashMap<>())
    );
  }

  // TODO: extend StreamsConfig ourselves to avoid depending on internal AK utils
  public static StreamsConfig quietReadOnlystreamsConfig(final Map<?, ?> configs) {
    // use a "quiet" StreamsConfig to avoid excessive repeat logging
    return new ClientUtils.QuietStreamsConfig(configs);
  }

  // TODO: move to ResponsiveStreamsConfig and define clear boundary between "real" config object
  //  containing the InternalConfig/SharedClients, and dummy quiet StreamsConfig for accessing
  //  pure Streams properties
  private static StreamsConfig verifiedStreamsConfigs(
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

    // In this case the default and our desired value are both 0, so we only need to check for
    // accidental user overrides
    final Integer numStandbys = (Integer) configs.get(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG);
    if (numStandbys != null && numStandbys != 0) {
      final String errorMsg = String.format(
          "Invalid Streams configuration value for '%s': got %d, expected '%d'",
          StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG,
          numStandbys,
          NUM_STANDBYS_OVERRIDE
      );
      LOG.error(errorMsg);
      throw new ConfigException(errorMsg);
    }

    // TODO(sophie): finish writing KIP to make this a public StreamsConfig
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

    final StreamsConfig streamsConfig = new StreamsConfig(propsWithOverrides);
    verifyNotEosV1(streamsConfig);
    StoreUtil.validateTopologyOptimizationConfig(streamsConfig);


    return streamsConfig;
  }

  @SuppressWarnings("deprecation")
  private static void verifyNotEosV1(final StreamsConfig streamsConfig) {
    if (EXACTLY_ONCE.equals(streamsConfig.getString(StreamsConfig.PROCESSING_GUARANTEE_CONFIG))) {
      throw new IllegalStateException("Responsive driver can only be used with ALOS/EOS-V2");
    }
  }

  @Override
  public void setStateListener(final StateListener stateListener) {
    super.setStateListener(stateListener);
    this.stateListener = stateListener;
  }

  public StateListener stateListener() {
    return stateListener;
  }

  /**
   * Sets a global state restore listener -- note that this is "global" in the sense that
   * it is set at the client-level and will be invoked for all stores in this topology, not that
   * it will be invoked for global state stores. This will only be invoked for ACTIVE task types,
   * ie not for global or standby tasks.
   *
   * @param listener The listener triggered when a {@link StateStore} is being restored.
   */
  @Override
  public void setGlobalStateRestoreListener(final StateRestoreListener listener) {
    restoreListener.registerUserRestoreListener(listener);
  }

  public StateRestoreListener userStateRestoreListener() {
    return restoreListener.userListener();
  }

  private void closeClients() {
    sharedClients.closeAll();
  }

  @Override
  public void close() {
    super.close();
    closeClients();
  }

  @Override
  public boolean close(final Duration timeout) {
    final boolean closed = super.close(timeout);
    closeClients();
    return closed;
  }

  @Override
  public boolean close(final CloseOptions options) {
    final boolean closed = super.close(options);
    closeClients();
    return closed;
  }
}
