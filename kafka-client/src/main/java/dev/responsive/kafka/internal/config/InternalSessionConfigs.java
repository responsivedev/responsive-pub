package dev.responsive.kafka.internal.config;

import static org.apache.kafka.streams.StreamsConfig.mainConsumerPrefix;

import dev.responsive.kafka.api.async.internals.AsyncThreadPoolRegistry;
import dev.responsive.kafka.internal.metrics.ResponsiveMetrics;
import dev.responsive.kafka.internal.stores.ResponsiveStoreRegistry;
import dev.responsive.kafka.internal.utils.SessionClients;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("checkstyle:linelength")
public final class InternalSessionConfigs {
  private static final Logger LOG = LoggerFactory.getLogger(InternalSessionConfigs.class);

  private static final String INTERNAL_ASYNC_THREAD_POOL_REGISTRY_CONFIG = "__internal.responsive.async.thread.pool.registry__";
  private static final String INTERNAL_SESSION_CLIENTS_CONFIG = "__internal.responsive.cassandra.client__";
  private static final String INTERNAL_STORE_REGISTRY_CONFIG = "__internal.responsive.store.registry__";
  private static final String INTERNAL_TOPOLOGY_DESCRIPTION_CONFIG = "__internal.responsive.topology.description__";
  private static final String INTERNAL_METRICS_CONFIG = "__internal.responsive.metrics__";

  private static <T> T loadFromConfig(
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

  public static boolean isAsyncThreadPoolRegistryEnabled(final Map<String, Object> configs) {
    return configs.containsKey(INTERNAL_ASYNC_THREAD_POOL_REGISTRY_CONFIG);
  }

  // CAUTION: this method assumes the provided config map has stripped away the
  // main.consumer prefix that was added to this config in the original Streams
  // properties. See the javadocs below for the Builder#withAsyncThreadPoolRegistry
  // method for more details on when it is safe to use this
  public static AsyncThreadPoolRegistry loadAsyncThreadPoolRegistry(final Map<String, Object> configs) {
    return loadFromConfig(
        configs,
        InternalSessionConfigs.INTERNAL_ASYNC_THREAD_POOL_REGISTRY_CONFIG,
        AsyncThreadPoolRegistry.class,
        "Async thread pool registry"
    );
  }

  public static ResponsiveMetrics loadMetrics(final Map<String, Object> configs) {
    return loadFromConfig(
        configs,
        InternalSessionConfigs.INTERNAL_METRICS_CONFIG,
        ResponsiveMetrics.class,
        "Responsive Metrics"
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

    /**
     * Note: we must add the main consumer prefix when first building the config
     * map with this registry, as it is needed in the #getMainConsumer method of the
     * KafkaClientSupplier, and only main-consumer configs are included in the copy
     * of the config map it receives.
     * Importantly, we should NOT include this prefix when attempting to retrieve this
     * registry on the other end, unless you are extracting it from the original, app-wide
     * config map. This prefix will be stripped away when the config is copied into a
     * prefix-based submap, such as the one passed in to the KafkaClientSupplier for
     * the main consumer or the one returned from the
     * {@link ProcessorContext#appConfigsWithPrefix(String)} API.
     * The {@link #loadAsyncThreadPoolRegistry(Map)} method assumes the prefix has been
     * stripped and therefore only works on filtered submaps like in the two
     * examples above
     */
    public Builder withAsyncThreadPoolRegistry(final AsyncThreadPoolRegistry registry) {
      configs.put(mainConsumerPrefix(INTERNAL_ASYNC_THREAD_POOL_REGISTRY_CONFIG), registry);
      return this;
    }

    public Builder withMetrics(final ResponsiveMetrics metrics) {
      configs.put(INTERNAL_METRICS_CONFIG, metrics);
      return this;
    }

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
