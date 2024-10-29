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

package dev.responsive.kafka.internal.db;

import static dev.responsive.kafka.api.config.ResponsiveConfig.CASSANDRA_DESIRED_NUM_PARTITION_CONFIG;
import static dev.responsive.kafka.testutils.IntegrationTestUtils.copyConfigWithOverrides;
import static java.util.Collections.singletonMap;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import dev.responsive.kafka.api.config.ResponsiveConfig;
import dev.responsive.kafka.api.stores.ResponsiveKeyValueParams;
import dev.responsive.kafka.api.stores.TtlProvider;
import dev.responsive.kafka.internal.db.partitioning.SubPartitioner;
import dev.responsive.kafka.internal.db.spec.DefaultTableSpec;
import dev.responsive.kafka.internal.stores.TtlResolver;
import dev.responsive.kafka.testutils.ResponsiveConfigParam;
import dev.responsive.kafka.testutils.ResponsiveExtension;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.CassandraContainer;

@ExtendWith(ResponsiveExtension.class)
public class CassandraKVTableIntegrationTest {

  // Set up with 4 subpartitions per kafka partition
  private static final int NUM_SUBPARTITIONS_TOTAL = 8;
  private static final int NUM_KAFKA_PARTITIONS = NUM_SUBPARTITIONS_TOTAL / 4;

  private CassandraClient client;
  private CqlSession session;
  private String storeName; // ie the "kafkaName", NOT the "cassandraName"
  private ResponsiveConfig config;

  @BeforeEach
  public void before(
      final TestInfo info,
      final CassandraContainer<?> cassandra,
      @ResponsiveConfigParam final ResponsiveConfig config
  ) throws InterruptedException, TimeoutException {
    this.session = CqlSession.builder()
        .addContactPoint(cassandra.getContactPoint())
        .withLocalDatacenter(cassandra.getLocalDatacenter())
        .withKeyspace("responsive_itests") // NOTE: this keyspace is expected to exist
        .build();

    this.client = new CassandraClient(session, config);
    this.storeName = info.getTestMethod().orElseThrow().getName();
    this.config = config;
  }

  private <K, V> RemoteKVTable<BoundStatement> createTable(
      final TtlProvider<K, V> ttlProvider
  ) {
    return createTableFromParams(
        ResponsiveKeyValueParams.keyValue(storeName).withTtlProvider(ttlProvider)
    );

  }

  private RemoteKVTable<BoundStatement> createTable() {
    return createTableFromParams(
        ResponsiveKeyValueParams.keyValue(storeName)
    );
  }

  private RemoteKVTable<BoundStatement> createTableFromParams(
      final ResponsiveKeyValueParams params
  ) {
    final String tableName = params.name().tableName();

    final ResponsiveConfig partitionerConfig = copyConfigWithOverrides(
        config,
        singletonMap(CASSANDRA_DESIRED_NUM_PARTITION_CONFIG, NUM_SUBPARTITIONS_TOTAL)
    );
    final var partitioner = SubPartitioner.create(
        OptionalInt.empty(),
        NUM_KAFKA_PARTITIONS,
        tableName,
        partitionerConfig,
        storeName + "-changelog"
    );

    final Optional<TtlResolver<?, ?>> ttlResolver = withTtlProvider(params.ttlProvider());

    try {
      return CassandraKeyValueTable.create(
          new DefaultTableSpec(tableName, partitioner, ttlResolver), client);
    } catch (final Exception e) {
      throw new AssertionError("Failed to create table", e);
    }
  }

  @Test
  public void shouldReturnAllKeysInLexicalOrderAcrossMultipleSubPartitions() {
    // Given:
    final RemoteKVTable<BoundStatement> table = createTable();

    final List<BoundStatement> inserts = List.of(
        table.insert(0, Bytes.wrap(new byte[]{0x0, 0x1}), new byte[]{0x1}, 0L),
        table.insert(0, Bytes.wrap(new byte[]{0x1, 0x0}), new byte[]{0x1}, 0L),
        table.insert(0, Bytes.wrap(new byte[]{0x2, 0x0}), new byte[]{0x1}, 0L),
        table.insert(0, Bytes.wrap(new byte[]{0x2, 0x2}), new byte[]{0x1}, 0L),
        table.insert(0, Bytes.wrap(new byte[]{0x1, 0x1}), new byte[]{0x1}, 0L),
        table.insert(0, Bytes.wrap(new byte[]{0x0, 0x2}), new byte[]{0x1}, 0L),
        table.insert(0, Bytes.wrap(new byte[]{0x2}), new byte[]{0x1}, 0L),
        table.insert(0, Bytes.wrap(new byte[]{0x0}), new byte[]{0x1}, 0L)
    );
    inserts.forEach(client::execute);

    // When:
    final KeyValueIterator<Bytes, byte[]> all = table.all(0, 0L);

    // Then:
    Bytes old = all.next().key;
    while (all.hasNext()) {
      final Bytes next = all.next().key;
      assertThat(next.compareTo(old), Matchers.greaterThan(0));
      old = next;
    }
  }

  @Test
  public void shouldReturnRangeKeysInLexicalOrderAcrossMultipleSubPartitions() {
    // Given:
    final RemoteKVTable<BoundStatement> table = createTable();

    final List<BoundStatement> inserts = List.of(
        table.insert(0, Bytes.wrap("A".getBytes()), new byte[]{0x1}, 0L),
        table.insert(0, Bytes.wrap("B".getBytes()), new byte[]{0x1}, 0L),
        table.insert(0, Bytes.wrap("C".getBytes()), new byte[]{0x1}, 0L),
        table.insert(0, Bytes.wrap("CC".getBytes()), new byte[]{0x1}, 0L),
        table.insert(0, Bytes.wrap("CCC".getBytes()), new byte[]{0x1}, 0L),
        table.insert(0, Bytes.wrap("CD".getBytes()), new byte[]{0x1}, 0L),
        table.insert(0, Bytes.wrap("D".getBytes()), new byte[]{0x1}, 0L),
        table.insert(0, Bytes.wrap("E".getBytes()), new byte[]{0x1}, 0L),
        table.insert(0, Bytes.wrap("F".getBytes()), new byte[]{0x1}, 0L),
        table.insert(0, Bytes.wrap("G".getBytes()), new byte[]{0x1}, 0L),
        table.insert(0, Bytes.wrap("H".getBytes()), new byte[]{0x1}, 0L),
        table.insert(0, Bytes.wrap("I".getBytes()), new byte[]{0x1}, 0L)
    );
    inserts.forEach(client::execute);

    try (final KeyValueIterator<Bytes, byte[]> range = table.range(
        0,
        Bytes.wrap("B".getBytes()),
        Bytes.wrap("H".getBytes()),
        0L)
    ) {

      // Then:
      final List<Bytes> keys = new ArrayList<>();
      range.forEachRemaining(kv -> keys.add(kv.key));
      assertThat(keys, hasSize(10));
      assertThat(keys.get(0), equalTo(Bytes.wrap("B".getBytes())));
      assertThat(keys.get(1), equalTo(Bytes.wrap("C".getBytes())));
      assertThat(keys.get(2), equalTo(Bytes.wrap("CC".getBytes())));
      assertThat(keys.get(3), equalTo(Bytes.wrap("CCC".getBytes())));
      assertThat(keys.get(4), equalTo(Bytes.wrap("CD".getBytes())));
      assertThat(keys.get(5), equalTo(Bytes.wrap("D".getBytes())));
      assertThat(keys.get(6), equalTo(Bytes.wrap("E".getBytes())));
      assertThat(keys.get(7), equalTo(Bytes.wrap("F".getBytes())));
      assertThat(keys.get(8), equalTo(Bytes.wrap("G".getBytes())));
      assertThat(keys.get(9), equalTo(Bytes.wrap("H".getBytes())));
    }
  }

  @SuppressWarnings("OptionalGetWithoutIsPresent")
  @Test
  public void shouldConfigureDefaultTtl()  {
    // Given:
    final long ttlMs = 100L;
    final TtlProvider<?, ?> ttlProvider = TtlProvider.withDefault(Duration.ofMillis(ttlMs));
    final ResponsiveKeyValueParams params =
        ResponsiveKeyValueParams.keyValue(storeName).withTtlProvider(ttlProvider);
    final String tableName = params.name().tableName();

    // When:
    createTableFromParams(params);

    // Then:
    final var table = session.getMetadata()
        .getKeyspace(session.getKeyspace().get())
        .get()
        .getTable(tableName)
        .get();
    final String describe = table.describe(false);

    final int ttlSeconds = (int) TimeUnit.MILLISECONDS.toSeconds(ttlMs);
    assertThat(describe, containsString("default_time_to_live = " + ttlSeconds));
  }

  @Test
  public void shouldRespectSemanticDefaultOnlyTtlForLookups() {
    // Given:
    final long ttlMs = 100L;
    final TtlProvider<?, ?> ttlProvider = TtlProvider.withDefault(Duration.ofMillis(ttlMs));
    final RemoteKVTable<BoundStatement> table = createTable(ttlProvider);

    // When:
    final long insertTimeMs = 0L;
    client.execute(
        table.insert(0, Bytes.wrap(new byte[]{0x0, 0x1}), new byte[]{0x1}, insertTimeMs));

    // Then:
    final long lookupTimeValid = ttlMs - 1L;
    final byte[] valid = table.get(0, Bytes.wrap(new byte[]{0x0, 0x1}), lookupTimeValid);

    final long lookupTimeExpired = ttlMs + 1L;
    final byte[] expired = table.get(0, Bytes.wrap(new byte[]{0x0, 0x1}), lookupTimeExpired);

    assertThat(valid, Matchers.is(new byte[]{0x1}));
    assertThat(expired, Matchers.nullValue());
  }

  @Test
  public void shouldRespectSemanticDefaultOnlyTtlForRangeQueries() {
    // Given:
    final long ttlMs = 100L;
    final TtlProvider<?, ?> ttlProvider = TtlProvider.withDefault(Duration.ofMillis(ttlMs));
    final RemoteKVTable<BoundStatement> table = createTable(ttlProvider);

    final List<BoundStatement> inserts = List.of(
        table.insert(0, Bytes.wrap(new byte[]{0x0, 0x0}), new byte[]{0x1}, 10L),
        table.insert(0, Bytes.wrap(new byte[]{0x0, 0x1}), new byte[]{0x1}, 0L), // expired
        table.insert(0, Bytes.wrap(new byte[]{0x0, 0x2}), new byte[]{0x1}, 20L),
        table.insert(0, Bytes.wrap(new byte[]{0x1, 0x0}), new byte[]{0x1}, 20L) // out of range
    );
    inserts.forEach(client::execute);

    // When:
    final long lookupTimeMs = ttlMs + 5L;
    final KeyValueIterator<Bytes, byte[]> range = table.range(
        0,
        Bytes.wrap(new byte[]{0x0, 0x0}),
        Bytes.wrap(new byte[]{0x0, 0x3}),
        lookupTimeMs
    );

    // Then:
    final List<Bytes> keys = new ArrayList<>();
    range.forEachRemaining(kv -> keys.add(kv.key));
    assertThat(keys, hasSize(2));
    assertThat(keys, Matchers.hasItems(
        Bytes.wrap(new byte[]{0x0, 0x0}),
        Bytes.wrap(new byte[]{0x0, 0x2})
    ));
  }

  @Test
  public void shouldRespectSemanticDefaultOnlyTtlForAllQueries() {
    // Given:
    final long ttlMs = MINUTES.toMillis(100);
    final TtlProvider<?, ?> ttlProvider = TtlProvider.withDefault(Duration.ofMillis(ttlMs));
    final RemoteKVTable<BoundStatement> table = createTable(ttlProvider);

    final List<BoundStatement> inserts = List.of(
        table.insert(0, Bytes.wrap(new byte[]{0x0, 0x0}), new byte[]{0x1}, MINUTES.toMillis(10L)),
        table.insert(0, Bytes.wrap(new byte[]{0x0, 0x1}), new byte[]{0x1}, MINUTES.toMillis(0L)), // expired
        table.insert(0, Bytes.wrap(new byte[]{0x0, 0x2}), new byte[]{0x1}, MINUTES.toMillis(20L))
    );
    inserts.forEach(client::execute);

    // When:
    final long lookupTimeMs = ttlMs + 5L;
    final KeyValueIterator<Bytes, byte[]> range = table.all(
        0,
        lookupTimeMs
    );

    // Then:
    final List<Bytes> keys = new ArrayList<>();
    range.forEachRemaining(kv -> keys.add(kv.key));
    assertThat(keys, hasSize(2));
    assertThat(keys, Matchers.hasItems(
        Bytes.wrap(new byte[]{0x0, 0x0}),
        Bytes.wrap(new byte[]{0x0, 0x2})
    ));
  }

  @Test
  public void shouldSupportDataKeyThatEqualsMetadataKey() {
    // Given:
    final RemoteKVTable<BoundStatement> table = createTable();

    final byte[] valBytes = new byte[]{0x1};
    client.execute(table.insert(0, ColumnName.METADATA_KEY, valBytes, 0L));

    // When:
    final byte[] val = table.get(0, ColumnName.METADATA_KEY, 0L);

    // Then:
    assertThat(val, Matchers.is(valBytes));
  }

}