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

package dev.responsive.kafka.api;

import dev.responsive.kafka.api.async.AsyncProcessorSupplier;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import net.bytebuddy.ByteBuddy;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyConfig;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.apache.kafka.streams.processor.internals.ProcessorTopology;
import org.apache.kafka.streams.processor.internals.StoreFactory;
import org.apache.kafka.streams.state.StoreBuilder;

public class ResponsiveStreamsBuilder extends StreamsBuilder {

  public ResponsiveStreamsBuilder() {
    // intercepted by new ResponsiveTopology
    super(null);
  }

  public ResponsiveStreamsBuilder(final TopologyConfig topologyConfigs) {
    super(topologyConfigs);
  }

  protected Topology newTopology(final TopologyConfig topologyConfigs) {
    return new ResponsiveTopology(topologyConfigs);
  }

  private static class ResponsiveTopology extends Topology {
    public ResponsiveTopology(final TopologyConfig config) {
      super(intercept(config));
    }

    static InternalTopologyBuilder intercept(final TopologyConfig config) {
      try {
        InternalTopologyBuilder builder;
        if (config == null) {
          builder = new ResponsiveTopologyBuilder();
        } else {
          builder = new ResponsiveTopologyBuilder(config);
        }
        return builder;
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  private static class ResponsiveTopologyBuilder extends InternalTopologyBuilder {

    public ResponsiveTopologyBuilder() {
      super();
    }

    public ResponsiveTopologyBuilder(final TopologyConfig config) {
      super(config);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public synchronized ProcessorTopology buildTopology() {
      try {
        Field nodeFactoriesField = InternalTopologyBuilder.class.getDeclaredField("nodeFactories");
        nodeFactoriesField.setAccessible(true);
        Map<String, Object> nodeFactories = (Map<String, Object>) nodeFactoriesField.get(this);

        Field stateFactoriesField = InternalTopologyBuilder.class.getDeclaredField("stateFactories");
        stateFactoriesField.setAccessible(true);
        Map<String, StoreFactory> stateFactories =
            (Map<String, StoreFactory>) stateFactoriesField.get(this);

        // Iterate through the map
        for (Map.Entry<String, ?> entry : nodeFactories.entrySet()) {
          String key = entry.getKey();
          Object nodeFactory = entry.getValue();

          final String clazz = nodeFactory.getClass().getName();
          if (clazz.contains("ProcessorNodeFactory") && !clazz.contains("Fixed")) {
            final Object proxy = proxyNodeFactory(key, nodeFactory, stateFactories);
            nodeFactories.put(key, proxy);
          }
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return super.buildTopology();
    }

    @SuppressWarnings("unchecked")
    private Object proxyNodeFactory(
        final String name,
        final Object nodeFactory,
        final Map<String, StoreFactory> stateFactories
    )
        throws NoSuchMethodException, InvocationTargetException, InstantiationException,
        IllegalAccessException, NoSuchFieldException {
      Field stateStoreNamesField = nodeFactory.getClass().getDeclaredField("stateStoreNames");
      stateStoreNamesField.setAccessible(true);
      Set<String> stateStoreNames = (Set<String>) stateStoreNamesField.get(nodeFactory);

      Field supplierField = nodeFactory.getClass().getDeclaredField("supplier");
      supplierField.setAccessible(true);
      ProcessorSupplier<?, ?, ?, ?> supplier = new AsyncProcessorSupplier<>(
          (ProcessorSupplier<?, ?, ?, ?>) supplierField.get(nodeFactory),
          stateFactories.entrySet()
              .stream()
              .filter(e -> stateStoreNames.contains(e.getKey()))
              .map(Map.Entry::getValue)
              .map(ReadOnlyStoreBuilder::new)
              .collect(
              Collectors.toSet())
      );

      Field predField = nodeFactory.getClass().getSuperclass().getDeclaredField("predecessors");
      predField.setAccessible(true);
      String[] predecessors = (String[]) predField.get(nodeFactory);

      final Object proxy = new ByteBuddy()
          .subclass(nodeFactory.getClass())
          .make()
          .load(InternalTopologyBuilder.class.getClassLoader(), net.bytebuddy.dynamic.loading.ClassLoadingStrategy.Default.INJECTION)
          .getLoaded()
          .getConstructor(String.class, String[].class, ProcessorSupplier.class)
          .newInstance(name, predecessors, supplier);

      stateStoreNamesField.set(proxy, stateStoreNames);
      return proxy;
    }
  }

  public static class ReadOnlyStoreBuilder<T extends StateStore> implements StoreBuilder<T> {

    public final StoreFactory factory;

    public ReadOnlyStoreBuilder(final StoreFactory factory) {
      this.factory = factory;
    }

    @Override
    public StoreBuilder<T> withCachingEnabled() {
      throw new UnsupportedOperationException("Should not attempt to modify StoreBuilder");
    }

    @Override
    public StoreBuilder<T> withCachingDisabled() {
      throw new UnsupportedOperationException("Should not attempt to modify StoreBuilder");
    }

    @Override
    public StoreBuilder<T> withLoggingEnabled(final Map<String, String> config) {
      throw new UnsupportedOperationException("Should not attempt to modify StoreBuilder");
    }

    @Override
    public StoreBuilder<T> withLoggingDisabled() {
      throw new UnsupportedOperationException("Should not attempt to modify StoreBuilder");
    }

    @SuppressWarnings("unchecked")
    @Override
    public T build() {
      return (T) factory.build();
    }

    @Override
    public Map<String, String> logConfig() {
      return factory.logConfig();
    }

    @Override
    public boolean loggingEnabled() {
      return factory.loggingEnabled();
    }

    @Override
    public String name() {
      return factory.name();
    }
  }

}
