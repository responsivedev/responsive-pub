package dev.responsive.kafka.clients;

import dev.responsive.db.CassandraClient;
import dev.responsive.kafka.api.InternalConfigs;
import java.util.Map;
import org.apache.kafka.clients.admin.Admin;

/**
 * Basic container class for session clients and other shared resources that should only
 * be closed when the app itself is shutdown
 */
public class SharedClients {
  public final CassandraClient cassandraClient;
  public final Admin admin;

  public static SharedClients loadSharedClients(final Map<String, Object> configs) {
    return new SharedClients(
        InternalConfigs.loadCassandraClient(configs),
        InternalConfigs.loadKafkaAdmin(configs)
    );
  }

  public SharedClients(final CassandraClient cassandraClient, final Admin admin) {
    this.cassandraClient = cassandraClient;
    this.admin = admin;
  }

  public void closeAll() {
    cassandraClient.shutdown();
    admin.close();
  }
}
