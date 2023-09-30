package dev.responsive.kafka.internal.db;

import static dev.responsive.kafka.api.config.ResponsiveConfig.CLIENT_ID_CONFIG;
import static dev.responsive.kafka.api.config.ResponsiveConfig.CLIENT_SECRET_CONFIG;
import static dev.responsive.kafka.api.config.ResponsiveConfig.MAX_CONCURRENT_REQUESTS_CONFIG;
import static dev.responsive.kafka.api.config.ResponsiveConfig.STORAGE_DATACENTER_CONFIG;
import static dev.responsive.kafka.api.config.ResponsiveConfig.STORAGE_HOSTNAME_CONFIG;
import static dev.responsive.kafka.api.config.ResponsiveConfig.STORAGE_PORT_CONFIG;
import static dev.responsive.kafka.api.config.ResponsiveConfig.TENANT_ID_CONFIG;

import com.datastax.oss.driver.api.core.CqlSession;
import dev.responsive.kafka.api.config.ResponsiveConfig;
import dev.responsive.kafka.internal.utils.SessionUtil;
import java.net.InetSocketAddress;
import org.apache.kafka.common.config.types.Password;

public class DefaultCassandraClientFactory implements CassandraClientFactory {
  @Override
  public CqlSession createCqlSession(final ResponsiveConfig config) {
    final InetSocketAddress address = InetSocketAddress.createUnresolved(
        config.getString(STORAGE_HOSTNAME_CONFIG),
        config.getInt(STORAGE_PORT_CONFIG)
    );
    final String datacenter = config.getString(STORAGE_DATACENTER_CONFIG);
    final String clientId = config.getString(CLIENT_ID_CONFIG);
    final Password clientSecret = config.getPassword(CLIENT_SECRET_CONFIG);
    final String tenant = config.getString(TENANT_ID_CONFIG);
    final int maxConcurrency = config.getInt(MAX_CONCURRENT_REQUESTS_CONFIG);

    return SessionUtil.connect(
        address,
        datacenter,
        tenant,
        clientId,
        clientSecret == null ? null : clientSecret.value(),
        maxConcurrency
    );
  }

  @Override
  public CassandraClient createCassandraClient(
      final CqlSession session,
      final ResponsiveConfig responsiveConfigs
  ) {
    return new CassandraClient(session, responsiveConfigs);
  }
}
