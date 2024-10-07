package dev.responsive.kafka.internal.db;

import com.datastax.oss.driver.api.core.CqlSession;
import dev.responsive.kafka.api.config.ResponsiveConfig;
import dev.responsive.kafka.internal.metrics.ResponsiveMetrics;
import javax.annotation.Nullable;

public interface CassandraClientFactory {

  CqlSession createCqlSession(
      final ResponsiveConfig config,
      @Nullable final ResponsiveMetrics metrics
  );

  CassandraClient createClient(
      final CqlSession session,
      final ResponsiveConfig config
  );
}
