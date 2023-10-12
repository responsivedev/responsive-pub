package dev.responsive.kafka.internal.db;

import com.datastax.oss.driver.api.core.CqlSession;
import dev.responsive.kafka.api.config.ResponsiveConfig;

public interface CassandraClientFactory {

  CqlSession createCqlSession(final ResponsiveConfig config);

  CassandraClient createClient(
      final CqlSession session,
      final ResponsiveConfig config
  );
}
