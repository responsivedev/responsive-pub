package dev.responsive.kafka.internal.utils;

import dev.responsive.kafka.internal.config.InternalConfigs;
import dev.responsive.kafka.internal.db.CassandraClient;
import dev.responsive.kafka.internal.metrics.ResponsiveRestoreListener;
import java.util.Map;
import org.apache.kafka.clients.admin.Admin;

/**
 * Basic container class for session clients and other shared resources that should only
 * be closed when the app itself is shutdown
 */
public class SharedClients {
  public final CassandraClient cassandraClient;
  public final Admin admin;
  public final ResponsiveRestoreListener restoreListener;

  public static SharedClients loadSharedClients(final Map<String, Object> configs) {
    return new SharedClients(
        InternalConfigs.loadCassandraClient(configs),
        InternalConfigs.loadKafkaAdmin(configs),
        InternalConfigs.loadRestoreListener(configs)
    );
  }

  public SharedClients(
      final CassandraClient cassandraClient,
      final Admin admin,
      final ResponsiveRestoreListener restoreListener
  ) {
    this.cassandraClient = cassandraClient;
    this.admin = admin;
    this.restoreListener = restoreListener;
  }

  public void closeAll() {
    cassandraClient.shutdown();
    admin.close();
    restoreListener.close();
  }
}
