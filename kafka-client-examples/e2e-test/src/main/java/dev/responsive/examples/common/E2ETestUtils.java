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

package dev.responsive.examples.common;

import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.REQUEST_TIMEOUT;
import static dev.responsive.kafka.api.config.ResponsiveConfig.CASSANDRA_HOSTNAME_CONFIG;
import static dev.responsive.kafka.api.config.ResponsiveConfig.CASSANDRA_PORT_CONFIG;
import static dev.responsive.kafka.api.config.ResponsiveConfig.STORAGE_BACKEND_TYPE_CONFIG;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.CreateKeyspace;
import dev.responsive.kafka.api.config.StorageBackend;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class E2ETestUtils {
  private static final Logger LOG = LoggerFactory.getLogger(E2ETestUtils.class);

  /**
   * Creates topics if they do not already exist.
   *
   * @param properties  configuration properties for the Kafka admin client
   * @param partitions  number of partitions for the topics
   * @param topics      list of topic names to create
   */
  public static void maybeCreateTopics(
      final Map<String, Object> properties,
      final int partitions,
      final List<String> topics
  ) {
    E2ETestUtils.retryFor(
        () -> doMaybeCreateTopics(properties, partitions, topics),
        Duration.ofMinutes(5)
    );
  }

  private static void doMaybeCreateTopics(
      final Map<String, Object> properties,
      final int partitions,
      final List<String> topics
  ) {
    try (final Admin admin = Admin.create(properties)) {
      for (final var topic : topics) {
        LOG.info("create topic {}", topic);
        try {
          admin.createTopics(List.of(new NewTopic(topic, partitions, (short) 1)))
              .all().get();
        } catch (final ExecutionException | InterruptedException e) {
          if (e.getCause() instanceof TopicExistsException) {
            continue;
          }
          throw new RuntimeException(e);
        } catch (final RuntimeException e) {
          LOG.info("Error creating topic: " + e);
        }
      }
    }
  }

  /**
   * Retries a task for a specified duration if it fails.
   *
   * @param task    the task to be executed
   * @param timeout the maximum duration to retry the task
   */
  public static void retryFor(final Runnable task, Duration timeout) {
    final Instant start = Instant.now();
    RuntimeException last = new IllegalStateException();
    while (Instant.now().isBefore(start.plus(timeout))) {
      try {
        task.run();
        return;
      } catch (final RuntimeException e) {
        last = e;
        LOG.error("task failed. retry in 3 seconds", e);
      }
      try {
        Thread.sleep(3000);
      } catch (final InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    throw last;
  }

  /**
   * Attempts to create a keyspace if the storage backend type is Cassandra.
   *
   * @param properties a map of configuration properties
   */
  public static void maybeCreateKeyspace(final Map<String, Object> properties) {
    if (properties.get(STORAGE_BACKEND_TYPE_CONFIG).equals(StorageBackend.CASSANDRA.name())) {
      E2ETestUtils.retryFor(() -> doMaybeCreateKeyspace(properties), Duration.ofMinutes(5));
    }
  }

  private static void doMaybeCreateKeyspace(final Map<String, Object> properties) {
    LOG.info("create keyspace responsive_test");
    try (final CqlSession session = cqlSession(properties)) {
      final CreateKeyspace createKeyspace = SchemaBuilder.createKeyspace("responsive_test")
          .ifNotExists()
          .withSimpleStrategy(3);
      session.execute(createKeyspace.build());
    }
  }

  private static CqlSession cqlSession(final Map<String, Object> properties) {
    final String scyllaName = properties.get(CASSANDRA_HOSTNAME_CONFIG).toString();
    final int port = Integer.parseInt(properties.get(CASSANDRA_PORT_CONFIG).toString());
    return CqlSession.builder()
        .addContactPoint(new InetSocketAddress(scyllaName, port))
        .withLocalDatacenter("datacenter1")
        .withConfigLoader(DriverConfigLoader.programmaticBuilder()
            .withLong(REQUEST_TIMEOUT, 100000)
            .build())
        .build();
  }

  public static void awaitKeyspace(final Map<String, Object> properties) {
    if (properties.get(STORAGE_BACKEND_TYPE_CONFIG).equals(StorageBackend.CASSANDRA.name())) {
      try (CqlSession session = cqlSession(properties)) {
        E2ETestUtils.retryFor(() -> keyspaceExists(session), Duration.ofMinutes(5));
      }
    }
  }

  private static void keyspaceExists(CqlSession session) {
    ResultSet resultSet = session.execute(
        SimpleStatement.builder(
                "SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name = ?")
            .addPositionalValue("responsive_test")
            .build());

    if (resultSet.one() == null) {
      throw new NoSuchElementException("keyspace responsive_test does not exist");
    }
  }

  public static Map<String, Object> defaultStreamProps(final Map<String, Object> originals) {
    final Map<String, Object> props = new HashMap<>(originals);
    props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
    props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);

    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);
    props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 60000);

    props.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 90000);
    return props;
  }

  private E2ETestUtils() {
  }

}
