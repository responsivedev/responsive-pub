package dev.responsive.kafka.api;

import dev.responsive.db.CassandraClient;
import dev.responsive.kafka.store.ResponsiveStoreRegistry;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
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
  private static final String INTERNAL_EXECUTOR_CLIENT_CONFIG =
              "__internal.responsive.executor.client__";

  private static final String TOPOLOGY_DESCRIPITON_CONFIG
      = "__internal.responsive.topology.description__";

  public static <T> T loadFromConfig(
      final Map<String, Object> configs,
      final String configName,
      final Class<T> type,
      final String name
  ) {
    final Object o = configs.get(configName);
    if (o == null) {
      final IllegalStateException fatalException =
          new IllegalStateException(name + " was missing");
      LOG.error(fatalException.getMessage(), fatalException);
      throw fatalException;
    } else if (!(type.isInstance(o))) {
      final IllegalStateException fatalException = new IllegalStateException(
          String.format("%s is not an instance of %s", o.getClass().getName(), type.getName())
      );
      LOG.error(fatalException.getMessage(), fatalException);
      throw fatalException;
    }
    return type.cast(o);
  }

  private InternalConfigs() {
  }

  public static TopologyDescription loadTopologyDescription(final Map<String, Object> config) {
    return loadFromConfig(
        config, TOPOLOGY_DESCRIPITON_CONFIG, TopologyDescription.class, "Topology description"
    );
  }

  public static CassandraClient loadCassandraClient(final Map<String, Object> configs) {
    return loadFromConfig(
        configs,
        InternalConfigs.INTERNAL_CASSANDRA_CLIENT_CONFIG,
        CassandraClient.class,
        "Shared Cassandra client"
    );
  }

  public static ScheduledExecutorService loadExecutorService(final Map<String, Object> configs) {
    return loadFromConfig(
        configs,
        InternalConfigs.INTERNAL_EXECUTOR_CLIENT_CONFIG,
        ScheduledExecutorService.class,
        "Shared ScheduledExecutorService client"
    );
  }

  public static Admin loadKafkaAdmin(final Map<String, Object> configs) {
    return loadFromConfig(
        configs,
        InternalConfigs.INTERNAL_ADMIN_CLIENT_CONFIG,
        Admin.class,
        "Shared Admin client"
    );
  }

  public static ResponsiveStoreRegistry loadStoreRegistry(final Map<String, Object> configs) {
    return loadFromConfig(
        configs,
        STORE_REGISTRY_CONFIG,
        ResponsiveStoreRegistry.class,
        "Store registry"
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

    public Builder withExecutorService(final ScheduledExecutorService executorService) {
      configs.put(INTERNAL_EXECUTOR_CLIENT_CONFIG, executorService);
      return this;
    }

    public Builder withStoreRegistry(final ResponsiveStoreRegistry storeRegistry) {
      configs.put(STORE_REGISTRY_CONFIG, storeRegistry);
      return this;
    }

    public Builder withTopologyDescription(final TopologyDescription topologyDescription) {
      configs.put(TOPOLOGY_DESCRIPITON_CONFIG, topologyDescription);
      return this;
    }

    public Map<String, Object> build() {
      return Map.copyOf(configs);
    }
  }

  public static Map<String, Object> getConfigs(
      final CassandraClient cassandraClient,
      final Admin admin,
      final ScheduledExecutorService executor,
      final ResponsiveStoreRegistry registry,
      final TopologyDescription topologyDescription
  ) {
    return Map.of(
        INTERNAL_CASSANDRA_CLIENT_CONFIG, cassandraClient,
        INTERNAL_ADMIN_CLIENT_CONFIG, admin,
        INTERNAL_EXECUTOR_CLIENT_CONFIG, executor,
        STORE_REGISTRY_CONFIG, registry,
        TOPOLOGY_DESCRIPITON_CONFIG, topologyDescription
    );
  }
}
