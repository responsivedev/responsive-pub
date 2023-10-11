package dev.responsive.kafka.internal.config;

import com.mongodb.client.MongoClient;
import dev.responsive.kafka.internal.db.CassandraClient;
import dev.responsive.kafka.internal.db.mongo.ResponsiveMongoClient;
import dev.responsive.kafka.internal.stores.ResponsiveStoreRegistry;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.streams.TopologyDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class InternalConfigs {
  private static final Logger LOG = LoggerFactory.getLogger(InternalConfigs.class);
  private static final String STORE_REGISTRY_CONFIG = "__internal.responsive.store.registry__";
  private static final String INTERNAL_CASSANDRA_CLIENT_CONFIG =
      "__internal.responsive.cassandra.client__";
  private static final String INTERNAL_ADMIN_CLIENT_CONFIG =
          "__internal.responsive.admin.client__";
  private static final String INTERNAL_MONGODB_CLIENT_CONFIG
      = "__internal.responsive.mongo.client__";

  private static final String TOPOLOGY_DESCRIPTION_CONFIG
      = "__internal.responsive.topology.description__";

  public static <T> Optional<T> loadFromConfig(
      final Map<String, Object> configs,
      final String configName,
      final Class<T> type,
      final String name
  ) {
    final Object o = configs.get(configName);
    if (o == null) {
      return Optional.empty();
    } else if (!(type.isInstance(o))) {
      final IllegalStateException fatalException = new IllegalStateException(
          String.format("%s is not an instance of %s", o.getClass().getName(), type.getName())
      );
      LOG.error(fatalException.getMessage(), fatalException);
      throw fatalException;
    }
    return Optional.of(type.cast(o));
  }

  private InternalConfigs() {
  }

  public static Optional<TopologyDescription> loadTopologyDescription(
      final Map<String, Object> config
  ) {
    return loadFromConfig(
        config, TOPOLOGY_DESCRIPTION_CONFIG, TopologyDescription.class, "Topology description"
    );
  }

  public static Optional<CassandraClient> loadCassandraClient(final Map<String, Object> configs) {
    return loadFromConfig(
        configs,
        InternalConfigs.INTERNAL_CASSANDRA_CLIENT_CONFIG,
        CassandraClient.class,
        "Shared Cassandra client"
    );
  }

  public static Admin loadKafkaAdmin(final Map<String, Object> configs) {
    final var admin = loadFromConfig(
        configs,
        InternalConfigs.INTERNAL_ADMIN_CLIENT_CONFIG,
        Admin.class,
        "Shared Admin client"
    );
    if (admin.isEmpty()) {
      final IllegalStateException fatalException =
          new IllegalStateException("Shared Admin client was missing");
      LOG.error(fatalException.getMessage(), fatalException);
      throw fatalException;
    }
    return admin.get();
  }

  public static ResponsiveStoreRegistry loadStoreRegistry(final Map<String, Object> configs) {
    final var registry = loadFromConfig(
        configs,
        STORE_REGISTRY_CONFIG,
        ResponsiveStoreRegistry.class,
        "Store registry"
    );
    if (registry.isEmpty()) {
      final IllegalStateException fatalException =
          new IllegalStateException("Store registry was missing");
      LOG.error(fatalException.getMessage(), fatalException);
      throw fatalException;
    }
    return registry.get();
  }

  public static Optional<ResponsiveMongoClient> loadMongoClient(final Map<String, Object> configs) {
    return loadFromConfig(
        configs,
        InternalConfigs.INTERNAL_MONGODB_CLIENT_CONFIG,
        ResponsiveMongoClient.class,
        "MongoDB Client"
    );
  }

  public static class Builder {
    private Map<String, Object> configs = new HashMap<>();

    public Builder withCassandraClient(final CassandraClient cassandraClient) {
      configs.put(INTERNAL_CASSANDRA_CLIENT_CONFIG, cassandraClient);
      return this;
    }

    public Builder withKafkaAdmin(final Admin admin) {
      configs.put(INTERNAL_ADMIN_CLIENT_CONFIG, admin);
      return this;
    }

    public Builder withStoreRegistry(final ResponsiveStoreRegistry storeRegistry) {
      configs.put(STORE_REGISTRY_CONFIG, storeRegistry);
      return this;
    }

    public Builder withTopologyDescription(final TopologyDescription topologyDescription) {
      configs.put(TOPOLOGY_DESCRIPTION_CONFIG, topologyDescription);
      return this;
    }

    public Builder withMongoDbClient(final ResponsiveMongoClient client) {
      configs.put(INTERNAL_MONGODB_CLIENT_CONFIG, client);
      return this;
    }

    public Map<String, Object> build() {
      return Map.copyOf(configs);
    }
  }

  public static Map<String, Object> getConfigs(
      final CassandraClient cassandraClient,
      final MongoClient mongoClient,
      final Admin admin,
      final ResponsiveStoreRegistry registry,
      final TopologyDescription topologyDescription
  ) {
    return Map.of(
        INTERNAL_CASSANDRA_CLIENT_CONFIG, cassandraClient,
        INTERNAL_MONGODB_CLIENT_CONFIG, mongoClient,
        INTERNAL_ADMIN_CLIENT_CONFIG, admin,
        STORE_REGISTRY_CONFIG, registry,
        TOPOLOGY_DESCRIPTION_CONFIG, topologyDescription
    );
  }
}
