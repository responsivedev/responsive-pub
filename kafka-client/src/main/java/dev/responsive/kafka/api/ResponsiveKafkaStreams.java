package dev.responsive.kafka.api;

import static dev.responsive.kafka.config.ResponsiveDriverConfig.CLIENT_ID_CONFIG;
import static dev.responsive.kafka.config.ResponsiveDriverConfig.CLIENT_SECRET_CONFIG;
import static dev.responsive.kafka.config.ResponsiveDriverConfig.STORAGE_DATACENTER_CONFIG;
import static dev.responsive.kafka.config.ResponsiveDriverConfig.STORAGE_HOSTNAME_CONFIG;
import static dev.responsive.kafka.config.ResponsiveDriverConfig.STORAGE_PORT_CONFIG;
import static dev.responsive.kafka.config.ResponsiveDriverConfig.TENANT_ID_CONFIG;
import static dev.responsive.kafka.config.ResponsiveStreamsConfig.verifiedStreamsConfigs;

import com.datastax.oss.driver.api.core.CqlSession;
import dev.responsive.db.CassandraClient;
import dev.responsive.kafka.clients.ResponsiveKafkaClientSupplier;
import dev.responsive.kafka.config.ResponsiveDriverConfig;
import dev.responsive.utils.SessionUtil;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

public class ResponsiveKafkaStreams extends KafkaStreams {

  private final CqlSession session;
  private final ScheduledExecutorService executor;
  private final Admin admin;

  private ResponsiveKafkaStreams(
      final Topology topology,
      final StreamsConfig streamsConfigs,
      final KafkaClientSupplier clientSupplier,
      final CqlSession session,
      final Admin admin,
      final ScheduledExecutorService executor
  ) {
    super(topology, streamsConfigs, clientSupplier);
    this.session = session;
    this.admin = admin;
    this.executor = executor;
  }

  public static ResponsiveKafkaStreams create(
      final Topology topology,
      final Map<String, Object> configs
  ) {
    return create(topology, configs, new ResponsiveKafkaClientSupplier());
  }

  public static ResponsiveKafkaStreams create(
      final Topology topology,
      final Map<String, Object> configs,
      final KafkaClientSupplier clientSupplier
  ) {
    final ResponsiveDriverConfig responsiveConfigs = new ResponsiveDriverConfig(configs);

    final InetSocketAddress address = InetSocketAddress.createUnresolved(
        responsiveConfigs.getString(STORAGE_HOSTNAME_CONFIG),
        responsiveConfigs.getInt(STORAGE_PORT_CONFIG)
    );

    final String datacenter = responsiveConfigs.getString(STORAGE_DATACENTER_CONFIG);
    final String clientId = responsiveConfigs.getString(CLIENT_ID_CONFIG);
    final Password clientSecret = responsiveConfigs.getPassword(CLIENT_SECRET_CONFIG);
    final String tenant = responsiveConfigs.getString(TENANT_ID_CONFIG);

    final CqlSession session = SessionUtil.connect(
        address,
        datacenter,
        tenant,
        clientId,
        clientSecret == null ? null : clientSecret.value()
    );

    final Admin admin = clientSupplier.getAdmin(configs);
    final ScheduledExecutorService executor = new ScheduledThreadPoolExecutor(2);

    return new ResponsiveKafkaStreams(
        topology,
        verifiedStreamsConfigs(configs, new CassandraClient(session), admin, executor),
        clientSupplier,
        session,
        admin,
        executor
    );
  }

  private void closeClients() {
    session.close();
    admin.close();
    executor.shutdown();
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
