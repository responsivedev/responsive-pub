package dev.responsive.kafka.api;

import com.datastax.oss.driver.api.core.CqlSession;
import dev.responsive.db.CassandraClient;
import dev.responsive.kafka.config.ResponsiveDriverConfig;
import java.util.Map;

public interface CassandraClientFactory {
  CqlSession createCqlSession(ResponsiveDriverConfig config);

  CassandraClient createCassandraClient(CqlSession session);
}
