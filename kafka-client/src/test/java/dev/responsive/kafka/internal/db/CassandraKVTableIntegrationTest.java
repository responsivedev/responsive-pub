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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import dev.responsive.kafka.api.config.ResponsiveConfig;
import dev.responsive.kafka.internal.db.partitioning.SubPartitioner;
import dev.responsive.kafka.internal.db.spec.DefaultTableSpec;
import dev.responsive.kafka.testutils.ResponsiveConfigParam;
import dev.responsive.kafka.testutils.ResponsiveExtension;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalInt;
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

  private static final long CURRENT_TS = 100L;
  private static final long MIN_VALID_TS = 0L;

  // Set up with 4 subpartitions per kafka partition
  private static final int NUM_SUBPARTITIONS_TOTAL = 8;
  private static final int NUM_KAFKA_PARTITIONS = NUM_SUBPARTITIONS_TOTAL / 4;

  private RemoteKVTable<BoundStatement> table;
  private CassandraClient client;

  @BeforeEach
  public void before(
      final TestInfo info,
      final CassandraContainer<?> cassandra,
      @ResponsiveConfigParam final ResponsiveConfig config
  ) throws InterruptedException, TimeoutException {
    final CqlSession session = CqlSession.builder()
        .addContactPoint(cassandra.getContactPoint())
        .withLocalDatacenter(cassandra.getLocalDatacenter())
        .withKeyspace("responsive_itests") // NOTE: this keyspace is expected to exist
        .build();
    client = new CassandraClient(session, config);

    final String name = info.getTestMethod().orElseThrow().getName();
    final ResponsiveConfig partitionerConfig = copyConfigWithOverrides(
        config,
        singletonMap(CASSANDRA_DESIRED_NUM_PARTITION_CONFIG, NUM_SUBPARTITIONS_TOTAL)
    );
    final var partitioner = SubPartitioner.create(
        OptionalInt.empty(),
        NUM_KAFKA_PARTITIONS,
        name,
        partitionerConfig,
        name + "-changelog"
    );
    table = CassandraKeyValueTable.create(
        new DefaultTableSpec(name, partitioner), client);
  }

  @Test
  public void shouldReturnAllKeysInLexicalOrderAcrossMultipleSubPartitions() {
    // Given:
    final List<BoundStatement> inserts = List.of(
        table.insert(0, Bytes.wrap(new byte[]{0x0, 0x1}), new byte[]{0x1}, CURRENT_TS),
        table.insert(0, Bytes.wrap(new byte[]{0x1, 0x0}), new byte[]{0x1}, CURRENT_TS),
        table.insert(0, Bytes.wrap(new byte[]{0x2, 0x0}), new byte[]{0x1}, CURRENT_TS),
        table.insert(0, Bytes.wrap(new byte[]{0x2, 0x2}), new byte[]{0x1}, CURRENT_TS),
        table.insert(0, Bytes.wrap(new byte[]{0x1, 0x1}), new byte[]{0x1}, CURRENT_TS),
        table.insert(0, Bytes.wrap(new byte[]{0x0, 0x2}), new byte[]{0x1}, CURRENT_TS),
        table.insert(0, Bytes.wrap(new byte[]{0x2}), new byte[]{0x1}, CURRENT_TS),
        table.insert(0, Bytes.wrap(new byte[]{0x0}), new byte[]{0x1}, CURRENT_TS)
    );
    inserts.forEach(client::execute);

    // When:
    final KeyValueIterator<Bytes, byte[]> all = table.all(0, MIN_VALID_TS);

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
    final List<BoundStatement> inserts = List.of(
        table.insert(0, Bytes.wrap("A".getBytes()), new byte[]{0x1}, CURRENT_TS),
        table.insert(0, Bytes.wrap("B".getBytes()), new byte[]{0x1}, CURRENT_TS),
        table.insert(0, Bytes.wrap("C".getBytes()), new byte[]{0x1}, CURRENT_TS),
        table.insert(0, Bytes.wrap("CC".getBytes()), new byte[]{0x1}, CURRENT_TS),
        table.insert(0, Bytes.wrap("CCC".getBytes()), new byte[]{0x1}, CURRENT_TS),
        table.insert(0, Bytes.wrap("CD".getBytes()), new byte[]{0x1}, CURRENT_TS),
        table.insert(0, Bytes.wrap("D".getBytes()), new byte[]{0x1}, CURRENT_TS),
        table.insert(0, Bytes.wrap("E".getBytes()), new byte[]{0x1}, CURRENT_TS),
        table.insert(0, Bytes.wrap("F".getBytes()), new byte[]{0x1}, CURRENT_TS),
        table.insert(0, Bytes.wrap("G".getBytes()), new byte[]{0x1}, CURRENT_TS),
        table.insert(0, Bytes.wrap("H".getBytes()), new byte[]{0x1}, CURRENT_TS),
        table.insert(0, Bytes.wrap("I".getBytes()), new byte[]{0x1}, CURRENT_TS)
    );
    inserts.forEach(client::execute);

    try (final KeyValueIterator<Bytes, byte[]> range = table.range(
        0,
        Bytes.wrap("B".getBytes()),
        Bytes.wrap("H".getBytes()),
        MIN_VALID_TS)
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

  @Test
  public void shouldRespectSemanticTtlForLookups() {
    // Given:
    client.execute(
        table.insert(0, Bytes.wrap(new byte[]{0x0, 0x1}), new byte[]{0x1}, CURRENT_TS));

    // When:
    final byte[] valid = table.get(0, Bytes.wrap(new byte[]{0x0, 0x1}), MIN_VALID_TS);
    final byte[] expired = table.get(0, Bytes.wrap(new byte[]{0x0, 0x1}), CURRENT_TS + 1);

    // Then:
    assertThat(valid, Matchers.is(new byte[]{0x1}));
    assertThat(expired, Matchers.nullValue());
  }

  @Test
  public void shouldRespectSemanticTtlForRangeQueries() {
    // Given:
    final List<BoundStatement> inserts = List.of(
        table.insert(0, Bytes.wrap(new byte[]{0x0, 0x0}), new byte[]{0x1}, CURRENT_TS + 10),
        // expired:
        table.insert(0, Bytes.wrap(new byte[]{0x0, 0x1}), new byte[]{0x1}, CURRENT_TS),
        table.insert(0, Bytes.wrap(new byte[]{0x0, 0x2}), new byte[]{0x1}, CURRENT_TS + 20),
        // not expired, but out of range:
        table.insert(0, Bytes.wrap(new byte[]{0x1, 0x0}), new byte[]{0x1}, CURRENT_TS + 20)
    );
    inserts.forEach(client::execute);

    // When:
    final KeyValueIterator<Bytes, byte[]> range = table.range(
        0,
        Bytes.wrap(new byte[]{0x0, 0x0}),
        Bytes.wrap(new byte[]{0x0, 0x3}),
        CURRENT_TS + 5
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
  public void shouldRespectSemanticTtlForAllQueries() {
    // Given:
    final List<BoundStatement> inserts = List.of(
        table.insert(0, Bytes.wrap(new byte[]{0x0, 0x0}), new byte[]{0x1}, CURRENT_TS + 10),
        // expired
        table.insert(0, Bytes.wrap(new byte[]{0x0, 0x1}), new byte[]{0x1}, CURRENT_TS),
        table.insert(0, Bytes.wrap(new byte[]{0x0, 0x2}), new byte[]{0x1}, CURRENT_TS + 20)
    );
    inserts.forEach(client::execute);

    // When:
    final KeyValueIterator<Bytes, byte[]> range = table.all(
        0,
        CURRENT_TS + 5
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
    final byte[] valBytes = new byte[]{0x1};
    client.execute(table.insert(0, ColumnName.METADATA_KEY, valBytes, CURRENT_TS));

    // When:
    final byte[] val = table.get(0, ColumnName.METADATA_KEY, MIN_VALID_TS);

    // Then:
    assertThat(val, Matchers.is(valBytes));
  }

}