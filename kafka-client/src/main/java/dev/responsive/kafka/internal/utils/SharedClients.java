package dev.responsive.kafka.internal.utils;

import dev.responsive.kafka.internal.config.InternalConfigs;
import dev.responsive.kafka.internal.db.CassandraClient;
import dev.responsive.kafka.internal.stores.ResponsiveStoreRegistry;
import java.util.Map;
import org.apache.kafka.clients.admin.Admin;

/**
 * Basic container class for session clients and other shared resources that should only
 * be closed when the app itself is shutdown
 */
public class SharedClients {
  public final CassandraClient cassandraClient;
  public final Admin admin;
  public final ResponsiveStoreRegistry storeRegistry;

  public static SharedClients loadSharedClients(final Map<String, Object> configs) {
    return new SharedClients(
        InternalConfigs.loadCassandraClient(configs),
        InternalConfigs.loadKafkaAdmin(configs),
        InternalConfigs.loadStoreRegistry(configs)
    );
  }

  public SharedClients(
      final CassandraClient cassandraClient,
      final Admin admin,
      final ResponsiveStoreRegistry storeRegistry
  ) {
    this.cassandraClient = cassandraClient;
    this.admin = admin;
    this.storeRegistry = storeRegistry;
  }

  public void closeAll() {
    cassandraClient.close();
    admin.close();
  }
}
