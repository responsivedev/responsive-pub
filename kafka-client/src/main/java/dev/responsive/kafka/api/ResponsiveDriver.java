/*
 * Copyright 2023 Responsive Computing, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dev.responsive.kafka.api;

import static dev.responsive.kafka.config.ResponsiveDriverConfig.CLIENT_ID_CONFIG;
import static dev.responsive.kafka.config.ResponsiveDriverConfig.CLIENT_SECRET_CONFIG;
import static dev.responsive.kafka.config.ResponsiveDriverConfig.STORAGE_DATACENTER_CONFIG;
import static dev.responsive.kafka.config.ResponsiveDriverConfig.STORAGE_HOSTNAME_CONFIG;
import static dev.responsive.kafka.config.ResponsiveDriverConfig.STORAGE_PORT_CONFIG;
import static dev.responsive.kafka.config.ResponsiveDriverConfig.TENANT_ID_CONFIG;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import dev.responsive.db.CassandraClient;
import dev.responsive.kafka.config.ResponsiveDriverConfig;
import dev.responsive.utils.SessionUtil;
import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowStore;

/**
 * The {@code ResponsiveDriver} should be instantiated once per JVM
 * and maintains a session and connection to the remote storage server.
 *
 * <p>It is also required to use this as a {@link KafkaClientSupplier} when
 * creating your Kafka Streams application to ensure that all advanced
 * Responsive functionality functions properly.</p>
 */
// TODO(agavra): we should put more thought into this API and consider splitting
// it up into a "reusable" session class and a "per-streams" driver so that we
// can properly track resources created by the driver
public class ResponsiveDriver implements StreamsStoreDriver, Closeable {

  private static final Map<String, String> CHANGELOG_CONFIG = Map.of(
      TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE);

  private final ScheduledExecutorService executor = new ScheduledThreadPoolExecutor(2);
  private final CqlSession session;
  private final CassandraClient client;
  private final Admin admin;
  private final StreamsConfig config;
  private final KafkaClientSupplier delegateClientSupplier = new DefaultKafkaClientSupplier();

  /**
   * @param props the properties to pass in
   * @return a new {@code ResponsiveDriver} and opens connections to remote Responsive servers
   */
  public static ResponsiveDriver connect(final Map<String, Object> props) {
    final Properties properties = new Properties();
    properties.putAll(props);
    return connect(properties);
  }

  /**
   * @param props the properties to pass in
   * @return a new {@code ResponsiveDriver} and opens connections to remote Responsive servers
   */
  public static ResponsiveDriver connect(final Properties props) {
    final ResponsiveDriverConfig configs = new ResponsiveDriverConfig(props);

    final InetSocketAddress address = InetSocketAddress.createUnresolved(
        configs.getString(STORAGE_HOSTNAME_CONFIG),
        configs.getInt(STORAGE_PORT_CONFIG)
    );

    final String datacenter = configs.getString(STORAGE_DATACENTER_CONFIG);
    final String clientId = configs.getString(CLIENT_ID_CONFIG);
    final Password clientSecret = configs.getPassword(CLIENT_SECRET_CONFIG);
    final String tenant = configs.getString(TENANT_ID_CONFIG);

    return new ResponsiveDriver(
        SessionUtil.connect(
            address,
            datacenter,
            tenant,
            clientId,
            clientSecret == null ? null : clientSecret.value()),
        Admin.create(props),
        new StreamsConfig(props));
  }

  @VisibleForTesting
  public ResponsiveDriver(
      final CqlSession session,
      final Admin admin,
      final StreamsConfig config
  ) {
    this.session = session;
    this.client = new CassandraClient(session);
    this.admin = admin;
    this.config = config;
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

  @Override
  public Admin getAdmin(final Map<String, Object> config) {
    return delegateClientSupplier.getAdmin(config);
  }

  @Override
  public Producer<byte[], byte[]> getProducer(final Map<String, Object> config) {
    return delegateClientSupplier.getProducer(config);
  }

  @Override
  public Consumer<byte[], byte[]> getConsumer(final Map<String, Object> config) {
    return delegateClientSupplier.getConsumer(config);
  }

  @Override
  public Consumer<byte[], byte[]> getRestoreConsumer(final Map<String, Object> config) {
    return delegateClientSupplier.getRestoreConsumer(config);
  }

  @Override
  public Consumer<byte[], byte[]> getGlobalConsumer(final Map<String, Object> config) {
    config.put(
        ConsumerConfig.GROUP_ID_CONFIG,
        this.config.getString(StreamsConfig.APPLICATION_ID_CONFIG) + "-global");
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    return new ResponsiveGlobalConsumer(
        config,
        delegateClientSupplier.getGlobalConsumer(config),
        admin
    );
  }

  @Override
  public void close() throws IOException {
    session.close();
    executor.shutdown();
  }
}