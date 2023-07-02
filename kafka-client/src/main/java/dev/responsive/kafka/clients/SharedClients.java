package dev.responsive.kafka.clients;

import dev.responsive.db.CassandraClient;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.kafka.clients.admin.Admin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Basic container class for session clients and other shared resources
 */
public class SharedClients {
  private static final String INTERNAL_CASSANDRA_CLIENT_CONFIG = "__internal.responsive.cassandra.client__";
  private static final String INTERNAL_ADMIN_CLIENT_CONFIG = "__internal.responsive.admin.client__";
  private static final String INTERNAL_EXECUTOR_CLIENT_CONFIG = "__internal.responsive.executor.client__";

  private static final Logger LOG = LoggerFactory.getLogger(SharedClients.class);

  public final CassandraClient cassandraClient;
  public final Admin admin;
  public final ScheduledExecutorService executor;

  public SharedClients(final Map<String, Object> configs) {
    {
      final Object o = configs.get(INTERNAL_CASSANDRA_CLIENT_CONFIG);
      if (o == null) {
        final IllegalStateException fatalException =
            new IllegalStateException("Shared Cassandra client was missing");
        LOG.error(fatalException.getMessage(), fatalException);
        throw fatalException;
      } else if (!(o instanceof CassandraClient)) {
        final IllegalStateException fatalException = new IllegalStateException(
            String.format("%s is not an instance of %s", 
                          o.getClass().getName(), CassandraClient.class.getName())
        );
        LOG.error(fatalException.getMessage(), fatalException);
        throw fatalException;
      } else {
        cassandraClient = (CassandraClient) o;
      }
    }

    {
      final Object o = configs.get(INTERNAL_ADMIN_CLIENT_CONFIG);
      if (o == null) {
        final IllegalStateException fatalException =
            new IllegalStateException("Shared Admin client was missing");
        LOG.error(fatalException.getMessage(), fatalException);
        throw fatalException;
      } else if (!(o instanceof Admin)) {
        final IllegalStateException fatalException = new IllegalStateException(
            String.format("%s is not an instance of %s", 
                          o.getClass().getName(), Admin.class.getName())
        );
        LOG.error(fatalException.getMessage(), fatalException);
        throw fatalException;
      } else {
        admin = (Admin) o;
      }
    }

    {
      final Object o = configs.get(INTERNAL_EXECUTOR_CLIENT_CONFIG);
      if (o == null) {
        final IllegalStateException fatalException =
            new IllegalStateException("Shared ScheduledExecutorService client was missing");
        LOG.error(fatalException.getMessage(), fatalException);
        throw fatalException;
      } else if (!(o instanceof ScheduledExecutorService)) {
        final IllegalStateException fatalException = new IllegalStateException(
            String.format("%s is not an instance of %s",
                          o.getClass().getName(), ScheduledExecutorService.class.getName())
        );
        LOG.error(fatalException.getMessage(), fatalException);
        throw fatalException;
      } else {
        executor = (ScheduledExecutorService) o;
      }
    }
  }

  public static Map<String, Object> sharedClientConfigs(
      final CassandraClient cassandraClient,
      final Admin admin,
      final ScheduledExecutorService executor
  ) {
    final Map<String, Object> configs = new HashMap<>();
    configs.put(INTERNAL_CASSANDRA_CLIENT_CONFIG, cassandraClient);
    configs.put(INTERNAL_ADMIN_CLIENT_CONFIG, admin);
    configs.put(INTERNAL_EXECUTOR_CLIENT_CONFIG, executor);
    return configs;
  }
}
