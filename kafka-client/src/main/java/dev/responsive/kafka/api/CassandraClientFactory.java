package dev.responsive.kafka.api;

import com.datastax.oss.driver.api.core.CqlSession;
import dev.responsive.db.CassandraClient;
import dev.responsive.kafka.config.ResponsiveConfig;

public interface CassandraClientFactory {
  CqlSession createCqlSession(ResponsiveConfig config);

  CassandraClient createCassandraClient(
      CqlSession session, final ResponsiveConfig responsiveConfigs);
}
