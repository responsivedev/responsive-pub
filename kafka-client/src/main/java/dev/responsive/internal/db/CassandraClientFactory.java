package dev.responsive.internal.db;

import com.datastax.oss.driver.api.core.CqlSession;
import dev.responsive.api.config.ResponsiveConfig;

public interface CassandraClientFactory {

  CqlSession createCqlSession(final ResponsiveConfig config);

  CassandraClient createCassandraClient(
      final CqlSession session,
      final ResponsiveConfig config
  );
}
