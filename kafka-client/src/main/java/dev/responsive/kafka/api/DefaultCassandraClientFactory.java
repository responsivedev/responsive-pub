package dev.responsive.kafka.api;

import static dev.responsive.kafka.config.ResponsiveConfig.CLIENT_ID_CONFIG;
import static dev.responsive.kafka.config.ResponsiveConfig.CLIENT_SECRET_CONFIG;
import static dev.responsive.kafka.config.ResponsiveConfig.STORAGE_DATACENTER_CONFIG;
import static dev.responsive.kafka.config.ResponsiveConfig.STORAGE_HOSTNAME_CONFIG;
import static dev.responsive.kafka.config.ResponsiveConfig.STORAGE_PORT_CONFIG;
import static dev.responsive.kafka.config.ResponsiveConfig.TENANT_ID_CONFIG;

import com.datastax.oss.driver.api.core.CqlSession;
import dev.responsive.db.CassandraClient;
import dev.responsive.kafka.config.ResponsiveConfig;
import dev.responsive.utils.SessionUtil;
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
    return SessionUtil.connect(
        address,
        datacenter,
        tenant,
        clientId,
        clientSecret == null ? null : clientSecret.value()
    );
  }

  @Override
  public CassandraClient createCassandraClient(final CqlSession session) {
    return new CassandraClient(session);
  }
}
