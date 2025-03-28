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

package dev.responsive.kafka.internal.db;

import static dev.responsive.kafka.api.config.ResponsiveConfig.CASSANDRA_DATACENTER_CONFIG;
import static dev.responsive.kafka.api.config.ResponsiveConfig.CASSANDRA_HOSTNAME_CONFIG;
import static dev.responsive.kafka.api.config.ResponsiveConfig.CASSANDRA_PASSWORD_CONFIG;
import static dev.responsive.kafka.api.config.ResponsiveConfig.CASSANDRA_PORT_CONFIG;
import static dev.responsive.kafka.api.config.ResponsiveConfig.CASSANDRA_USERNAME_CONFIG;
import static dev.responsive.kafka.api.config.ResponsiveConfig.MAX_CONCURRENT_REQUESTS_CONFIG;

import com.datastax.oss.driver.api.core.CqlSession;
import dev.responsive.kafka.api.config.ResponsiveConfig;
import dev.responsive.kafka.internal.config.ConfigUtils;
import dev.responsive.kafka.internal.metrics.ResponsiveMetrics;
import dev.responsive.kafka.internal.utils.SessionUtil;
import java.net.InetSocketAddress;
import org.apache.kafka.common.config.types.Password;

public class DefaultCassandraClientFactory implements CassandraClientFactory {
  @Override
  public CqlSession createCqlSession(
      final ResponsiveConfig config,
      final ResponsiveMetrics metrics
  ) {
    final InetSocketAddress address = InetSocketAddress.createUnresolved(
        config.getString(CASSANDRA_HOSTNAME_CONFIG),
        config.getInt(CASSANDRA_PORT_CONFIG)
    );

    final String datacenter = config.getString(CASSANDRA_DATACENTER_CONFIG);
    final String username = config.getString(CASSANDRA_USERNAME_CONFIG);
    final Password password = config.getPassword(CASSANDRA_PASSWORD_CONFIG);
    final String keyspace = ConfigUtils.cassandraKeyspace(config);
    final int maxConcurrency = config.getInt(MAX_CONCURRENT_REQUESTS_CONFIG);

    return SessionUtil.connect(
        address,
        datacenter,
        keyspace,
        username,
        password == null ? null : password.value(),
        maxConcurrency,
        metrics
    );
  }

  @Override
  public CassandraClient createClient(
      final CqlSession session,
      final ResponsiveConfig responsiveConfigs
  ) {
    return new CassandraClient(session, responsiveConfigs);
  }
}
