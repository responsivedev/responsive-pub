/*
 * Copyright 2024 Responsive Computing, Inc.
 *
 * This source code is licensed under the Responsive Business Source License Agreement v1.0
 * available at:
 *
 * https://www.responsive.dev/legal/responsive-bsl-10
 *
 * This software requires a valid Commercial License Key for production use. Trial and commercial
 * licenses can be obtained at https://www.responsive.dev
 */

package dev.responsive.kafka.internal.config;

import dev.responsive.kafka.api.async.internals.AsyncThreadPoolRegistry;
import dev.responsive.kafka.api.config.ResponsiveConfig;
import dev.responsive.kafka.internal.metrics.ResponsiveMetrics;
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

  private static final String INTERNAL_ASYNC_THREAD_POOL_REGISTRY_CONFIG = "__internal.responsive.async.thread.pool.registry__";
  private static final String INTERNAL_SESSION_CLIENTS_CONFIG = "__internal.responsive.session.clients__";
  private static final String INTERNAL_STORE_REGISTRY_CONFIG = "__internal.responsive.store.registry__";
  private static final String INTERNAL_TOPOLOGY_DESCRIPTION_CONFIG = "__internal.responsive.topology.description__";
  private static final String INTERNAL_METRICS_CONFIG = "__internal.responsive.metrics__";

  private static final String NOT_RESPONSIVE_ERR_MSG =
      "This can happen if you instantiated a Responsive state store with a KafkaStreams instance "
          + "that is not ResponsiveKafkaStreams.";

  private static <T> T loadFromConfig(
      final Map<String, Object> configs,
      final String configName,
      final Class<T> type,
      final String name,
      final Object details
  ) {
    final Object o = configs.get(configName);
    if (o == null) {
      final IllegalStateException fatalException = new IllegalStateException(
          String.format("Failed to load %s as %s was missing. %s", name, configName, details)
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
        "Topology description",
        NOT_RESPONSIVE_ERR_MSG
    );
  }

  public static AsyncThreadPoolRegistry loadAsyncThreadPoolRegistry(final Map<String, Object> configs) {
    return loadFromConfig(
        configs,
        InternalSessionConfigs.INTERNAL_ASYNC_THREAD_POOL_REGISTRY_CONFIG,
        AsyncThreadPoolRegistry.class,
        "Async thread pool registry",
        "This can happen if " + ResponsiveConfig.ASYNC_THREAD_POOL_SIZE_CONFIG
            + " is zero or an async store was registered with a KafkaStreams instance "
            + "that is not ResponsiveKafkaStreams"
    );
  }

  public static ResponsiveMetrics loadMetrics(final Map<String, Object> configs) {
    return loadFromConfig(
        configs,
        InternalSessionConfigs.INTERNAL_METRICS_CONFIG,
        ResponsiveMetrics.class,
        "Responsive Metrics",
        NOT_RESPONSIVE_ERR_MSG
    );
  }

  public static SessionClients loadSessionClients(final Map<String, Object> configs) {
    return loadFromConfig(
        configs,
        InternalSessionConfigs.INTERNAL_SESSION_CLIENTS_CONFIG,
        SessionClients.class,
        "Shared session clients",
        NOT_RESPONSIVE_ERR_MSG
    );
  }

  public static ResponsiveStoreRegistry loadStoreRegistry(final Map<String, Object> configs) {
    return loadFromConfig(
        configs,
        INTERNAL_STORE_REGISTRY_CONFIG,
        ResponsiveStoreRegistry.class,
        "Store registry",
        NOT_RESPONSIVE_ERR_MSG
    );
  }

  public static class Builder {
    private final Map<String, Object> configs = new HashMap<>();

    public Builder withAsyncThreadPoolRegistry(final AsyncThreadPoolRegistry registry) {
      configs.put(INTERNAL_ASYNC_THREAD_POOL_REGISTRY_CONFIG, registry);
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
