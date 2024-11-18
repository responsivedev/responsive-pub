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
