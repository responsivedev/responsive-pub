package dev.responsive.kafka.api;

import com.datastax.oss.driver.api.core.CqlSession;
import dev.responsive.db.CassandraClient;
import dev.responsive.kafka.config.ResponsiveConfig;
import org.apache.kafka.clients.admin.Admin;

public interface CassandraClientFactory {
  CqlSession createCqlSession(ResponsiveConfig config);

  CassandraClient createCassandraClient(
      CqlSession session,
      ResponsiveConfig responsiveConfigs,
      Admin admin
  );
}
