package dev.responsive.kafka.clients;

import dev.responsive.db.CassandraClient;
import dev.responsive.kafka.api.InternalConfigs;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.kafka.clients.admin.Admin;

/**
 * Basic container class for session clients and other shared resources
 */
public class SharedClients {
  public final CassandraClient cassandraClient;
  public final Admin admin;
  public final ScheduledExecutorService executor;

  public SharedClients(final Map<String, Object> configs) {
    cassandraClient = InternalConfigs.loadCassandraClient(configs);
    admin = InternalConfigs.loadKafkaAdmin(configs);
    executor = InternalConfigs.loadExecutorService(configs);
  }

}
