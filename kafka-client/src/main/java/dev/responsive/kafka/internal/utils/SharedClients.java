package dev.responsive.kafka.internal.utils;

import dev.responsive.kafka.api.config.StorageBackend;
import dev.responsive.kafka.internal.config.InternalConfigs;
import dev.responsive.kafka.internal.db.CassandraClient;
import dev.responsive.kafka.internal.db.mongo.ResponsiveMongoClient;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.clients.admin.Admin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Basic container class for session clients and other shared resources that should only
 * be closed when the app itself is shutdown
 */
public class SharedClients {

  private final Optional<ResponsiveMongoClient> mongoClient;
  private final Optional<CassandraClient> cassandraClient;
  private final Admin admin;

  private static final Logger LOG = LoggerFactory.getLogger(SharedClients.class);

  public static SharedClients loadSharedClients(final Map<String, Object> configs) {
    return new SharedClients(
        InternalConfigs.loadMongoClient(configs),
        InternalConfigs.loadCassandraClient(configs),
        InternalConfigs.loadKafkaAdmin(configs)
    );
  }

  public SharedClients(
      final Optional<ResponsiveMongoClient> mongoClient,
      final Optional<CassandraClient> cassandraClient,
      final Admin admin
  ) {
    this.mongoClient = mongoClient;
    this.cassandraClient = cassandraClient;
    this.admin = admin;


  }

  public void closeAll() {
    cassandraClient.ifPresent(CassandraClient::shutdown);
    mongoClient.ifPresent(ResponsiveMongoClient::close);
    admin().close();
  }

  public StorageBackend storageBackend() {
    if (mongoClient.isPresent()) {
      return StorageBackend.MONGO_DB;
    } else if (cassandraClient.isPresent()) {
      return StorageBackend.CASSANDRA;
    } else {
      throw new IllegalArgumentException("Invalid Shared Clients Configuration");
    }
  }

  public ResponsiveMongoClient mongoClient() {
    if (mongoClient.isEmpty()) {
      final IllegalStateException fatalException =
          new IllegalStateException("MongoDB client was missing");
      LOG.error(fatalException.getMessage(), fatalException);
      throw fatalException;
    }

    return mongoClient.get();
  }

  public CassandraClient cassandraClient() {
    if (cassandraClient.isEmpty()) {
      final IllegalStateException fatalException =
          new IllegalStateException("Cassandra client was missing");
      LOG.error(fatalException.getMessage(), fatalException);
      throw fatalException;
    }

    return cassandraClient.get();
  }

  public Admin admin() {
    return admin;
  }
}
