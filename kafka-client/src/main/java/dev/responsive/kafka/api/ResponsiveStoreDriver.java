package dev.responsive.kafka.api;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowStore;

import dev.responsive.db.CassandraClient;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

public class ResponsiveStoreDriver implements StreamsStoreDriver {
  private static final Map<String, String> CHANGELOG_CONFIG = Map.of(
      TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE);

  private final ScheduledExecutorService executor;
  private final CassandraClient client;
  private final Admin admin;

  ResponsiveStoreDriver(
      final CassandraClient client,
      final ScheduledExecutorService executor,
      final Admin admin
  ) {
    this.client = client;
    this.admin = admin;
    this.executor = executor;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public KeyValueBytesStoreSupplier kv(final String name) {
    return new ResponsiveKeyValueBytesStoreSupplier(client, name, executor, admin);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public WindowBytesStoreSupplier windowed(
      final String name,
      final long retentionMs,
      final long windowSize,
      final boolean retainDuplicates
  ) {
    return new ResponsiveWindowedStoreSupplier(
        client,
        name,
        executor,
        admin,
        retentionMs,
        windowSize,
        retainDuplicates
    );
  }

  @Override
  public KeyValueBytesStoreSupplier globalKv(final String name) {
    return new ResponsiveGlobalKeyValueBytesStoreSupplier(client, name, executor);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <K, V> Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized(
      final String name
  ) {
    return Materialized.<K, V>as(kv(name))
        .withLoggingEnabled(CHANGELOG_CONFIG);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <K, V> Materialized<K, V, WindowStore<Bytes, byte[]>> materialized(
      final String name,
      final long retentionMs,
      final long windowSize,
      final boolean retainDuplicates
  ) {
    return Materialized.<K, V>as(windowed(name, retentionMs, windowSize, retainDuplicates))
        .withLoggingEnabled(CHANGELOG_CONFIG);
  }

  @Override
  public <K, V> Materialized<K, V, KeyValueStore<Bytes, byte[]>> globalMaterialized(
      final String name
  ) {
    return Materialized.<K, V>as(globalKv(name))
        .withCachingDisabled();
  }
}
