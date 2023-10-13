package dev.responsive.kafka.internal.config;

import dev.responsive.kafka.internal.stores.ResponsiveStoreRegistry;
import dev.responsive.kafka.internal.utils.SessionClients;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.streams.TopologyDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("checkstyle:linelength")
public final class InternalSessionConfigs {
  private static final Logger LOG = LoggerFactory.getLogger(InternalSessionConfigs.class);

  private static final String INTERNAL_SESSION_CLIENTS_CONFIG = "__internal.responsive.cassandra.client__";
  private static final String INTERNAL_STORE_REGISTRY_CONFIG = "__internal.responsive.store.registry__";
  private static final String INTERNAL_TOPOLOGY_DESCRIPTION_CONFIG = "__internal.responsive.topology.description__";

  public static <T> T loadFromConfig(
      final Map<String, Object> configs,
      final String configName,
      final Class<T> type,
      final String name
  ) {
    final Object o = configs.get(configName);
    if (o == null) {
      final IllegalStateException fatalException = new IllegalStateException(
          String.format("Failed to load %s as %s was missing", name, configName)
      );
      LOG.error(fatalException.getMessage(), fatalException);
      throw fatalException;
    } else if (!(type.isInstance(o))) {
      final IllegalStateException fatalException = new IllegalStateException(
          String.format("Failed to load %s as %s is not an instance of %s",
                        name, o.getClass().getName(), type.getName())
      );
      LOG.error(fatalException.getMessage(), fatalException);
      throw fatalException;
    }
    return type.cast(o);
  }

  private InternalSessionConfigs() {
  }

  public static TopologyDescription loadTopologyDescription(
      final Map<String, Object> config
  ) {
    return loadFromConfig(
        config,
        INTERNAL_TOPOLOGY_DESCRIPTION_CONFIG,
        TopologyDescription.class,
        "Topology description"
    );
  }

  public static SessionClients loadSessionClients(final Map<String, Object> configs) {
    return loadFromConfig(
        configs,
        InternalSessionConfigs.INTERNAL_SESSION_CLIENTS_CONFIG,
        SessionClients.class,
        "Shared session clients"
    );
  }

  public static ResponsiveStoreRegistry loadStoreRegistry(final Map<String, Object> configs) {
    return loadFromConfig(
        configs,
        INTERNAL_STORE_REGISTRY_CONFIG,
        ResponsiveStoreRegistry.class,
        "Store registry"
    );
  }

  public static class Builder {
    private final Map<String, Object> configs = new HashMap<>();

    public Builder withSessionClients(final SessionClients sessionClients) {
      configs.put(INTERNAL_SESSION_CLIENTS_CONFIG, sessionClients);
      return this;
    }

    public Builder withStoreRegistry(final ResponsiveStoreRegistry storeRegistry) {
      configs.put(INTERNAL_STORE_REGISTRY_CONFIG, storeRegistry);
      return this;
    }

    public Builder withTopologyDescription(final TopologyDescription topologyDescription) {
      configs.put(INTERNAL_TOPOLOGY_DESCRIPTION_CONFIG, topologyDescription);
      return this;
    }

    public Map<String, Object> build() {
      return Map.copyOf(configs);
    }
  }

}
