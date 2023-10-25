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

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import dev.responsive.kafka.api.config.ResponsiveConfig;
import dev.responsive.kafka.internal.db.partitioning.SubPartitioner;
import dev.responsive.kafka.internal.db.spec.BaseTableSpec;
import dev.responsive.kafka.testutils.ResponsiveConfigParam;
import dev.responsive.kafka.testutils.ResponsiveExtension;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalInt;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.hamcrest.MatcherAssert;
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

  private RemoteKVTable<BoundStatement> table;
  private String name;
  private CassandraClient client;

  @BeforeEach
  public void before(
      final TestInfo info,
      final CassandraContainer<?> cassandra,
      @ResponsiveConfigParam final ResponsiveConfig config
  ) throws InterruptedException, TimeoutException {
    // NOTE: this keyspace is expected to exist
    CqlSession session = CqlSession.builder()
        .addContactPoint(cassandra.getContactPoint())
        .withLocalDatacenter(cassandra.getLocalDatacenter())
        .withKeyspace("responsive_clients") // NOTE: this keyspace is expected to exist
        .build();
    client = new CassandraClient(session, config);
    name = info.getTestMethod().orElseThrow().getName();
    final var partitioner = SubPartitioner.create(
        OptionalInt.empty(),
        1,
        name,
        config,
        name + "-changelog"
    );
    table = CassandraKeyValueTable.create(
        new BaseTableSpec(name, partitioner), client);
  }

  @Test
  public void shouldReturnAllKeysInLexicalOrder() {
    // Given:
    final String table = name;
    final List<BoundStatement> inserts = List.of(
        this.table.insert(0, Bytes.wrap(new byte[]{0x0, 0x1}), new byte[]{0x1}, CURRENT_TS),
        this.table.insert(0, Bytes.wrap(new byte[]{0x1, 0x0}), new byte[]{0x1}, CURRENT_TS),
        this.table.insert(0, Bytes.wrap(new byte[]{0x2, 0x0}), new byte[]{0x1}, CURRENT_TS),
        this.table.insert(0, Bytes.wrap(new byte[]{0x2, 0x2}), new byte[]{0x1}, CURRENT_TS),
        this.table.insert(0, Bytes.wrap(new byte[]{0x1, 0x1}), new byte[]{0x1}, CURRENT_TS),
        this.table.insert(0, Bytes.wrap(new byte[]{0x0, 0x2}), new byte[]{0x1}, CURRENT_TS),
        this.table.insert(0, Bytes.wrap(new byte[]{0x2}), new byte[]{0x1}, CURRENT_TS),
        this.table.insert(0, Bytes.wrap(new byte[]{0x0}), new byte[]{0x1}, CURRENT_TS)
    );
    inserts.forEach(client::execute);

    // When:
    final KeyValueIterator<Bytes, byte[]> all = this.table.all(0, MIN_VALID_TS);

    // Then:
    Bytes old = all.next().key;
    while (all.hasNext()) {
      final Bytes next = all.next().key;
      MatcherAssert.assertThat(next.compareTo(old), Matchers.greaterThan(0));
      old = next;
    }
  }

  @Test
  public void shouldReturnRangeKeysInLexicalOrder() {
    // Given:
    final List<BoundStatement> inserts = List.of(
        this.table.insert(0, Bytes.wrap(new byte[]{0x0, 0x1}), new byte[]{0x1}, CURRENT_TS),
        this.table.insert(0, Bytes.wrap(new byte[]{0x1, 0x0}), new byte[]{0x1}, CURRENT_TS),
        this.table.insert(0, Bytes.wrap(new byte[]{0x2, 0x0}), new byte[]{0x1}, CURRENT_TS),
        this.table.insert(0, Bytes.wrap(new byte[]{0x2, 0x2}), new byte[]{0x1}, CURRENT_TS),
        this.table.insert(0, Bytes.wrap(new byte[]{0x1, 0x1}), new byte[]{0x1}, CURRENT_TS),
        this.table.insert(0, Bytes.wrap(new byte[]{0x0, 0x2}), new byte[]{0x1}, CURRENT_TS),
        this.table.insert(0, Bytes.wrap(new byte[]{0x2}), new byte[]{0x1}, CURRENT_TS),
        this.table.insert(0, Bytes.wrap(new byte[]{0x0}), new byte[]{0x1}, CURRENT_TS)
    );
    inserts.forEach(client::execute);

    // When:
    final KeyValueIterator<Bytes, byte[]> range = this.table.range(
        0,
        Bytes.wrap(new byte[]{0x0, 0x1}),
        Bytes.wrap(new byte[]{0x0, 0x3}),
        MIN_VALID_TS);

    // Then:
    final List<Bytes> keys = new ArrayList<>();
    range.forEachRemaining(kv -> keys.add(kv.key));
    MatcherAssert.assertThat(keys, Matchers.hasSize(2));
    MatcherAssert.assertThat(keys, Matchers.hasItems(
        Bytes.wrap(new byte[]{0x0, 0x1}),
        Bytes.wrap(new byte[]{0x0, 0x2})
    ));
  }

  @Test
  public void shouldRespectSemanticTtlForLookups() {
    // Given:
    final String table = name;
    client.execute(
        this.table.insert(0, Bytes.wrap(new byte[]{0x0, 0x1}), new byte[]{0x1}, CURRENT_TS));

    // When:
    final byte[] valid = this.table.get(0, Bytes.wrap(new byte[]{0x0, 0x1}), MIN_VALID_TS);
    final byte[] expired = this.table.get(0, Bytes.wrap(new byte[]{0x0, 0x1}), CURRENT_TS + 1);

    // Then:
    MatcherAssert.assertThat(valid, Matchers.is(new byte[]{0x1}));
    MatcherAssert.assertThat(expired, Matchers.nullValue());
  }

  @Test
  public void shouldRespectSemanticTtlForRangeQueries() {
    // Given:
    final String table = name;
    final List<BoundStatement> inserts = List.of(
        this.table.insert(0, Bytes.wrap(new byte[]{0x0, 0x0}), new byte[]{0x1}, CURRENT_TS + 10),
        // expired
        this.table.insert(0, Bytes.wrap(new byte[]{0x0, 0x1}), new byte[]{0x1}, CURRENT_TS),
        this.table.insert(0, Bytes.wrap(new byte[]{0x0, 0x2}), new byte[]{0x1}, CURRENT_TS + 20),
        // not expired, but out of range
        this.table.insert(0, Bytes.wrap(new byte[]{0x1, 0x0}), new byte[]{0x1}, CURRENT_TS + 20)
    );
    inserts.forEach(client::execute);

    // When:
    final KeyValueIterator<Bytes, byte[]> range = this.table.range(
        0,
        Bytes.wrap(new byte[]{0x0, 0x0}),
        Bytes.wrap(new byte[]{0x0, 0x3}),
        CURRENT_TS + 5
    );

    // Then:
    final List<Bytes> keys = new ArrayList<>();
    range.forEachRemaining(kv -> keys.add(kv.key));
    MatcherAssert.assertThat(keys, Matchers.hasSize(2));
    MatcherAssert.assertThat(keys, Matchers.hasItems(
        Bytes.wrap(new byte[]{0x0, 0x0}),
        Bytes.wrap(new byte[]{0x0, 0x2})
    ));
  }

  @Test
  public void shouldRespectSemanticTtlForAllQueries() {
    // Given:
    final String table = name;
    final List<BoundStatement> inserts = List.of(
        this.table.insert(0, Bytes.wrap(new byte[]{0x0, 0x0}), new byte[]{0x1}, CURRENT_TS + 10),
        // expired
        this.table.insert(0, Bytes.wrap(new byte[]{0x0, 0x1}), new byte[]{0x1}, CURRENT_TS),
        this.table.insert(0, Bytes.wrap(new byte[]{0x0, 0x2}), new byte[]{0x1}, CURRENT_TS + 20)
    );
    inserts.forEach(client::execute);

    // When:
    final KeyValueIterator<Bytes, byte[]> range = this.table.all(
        0,
        CURRENT_TS + 5
    );

    // Then:
    final List<Bytes> keys = new ArrayList<>();
    range.forEachRemaining(kv -> keys.add(kv.key));
    MatcherAssert.assertThat(keys, Matchers.hasSize(2));
    MatcherAssert.assertThat(keys, Matchers.hasItems(
        Bytes.wrap(new byte[]{0x0, 0x0}),
        Bytes.wrap(new byte[]{0x0, 0x2})
    ));
  }

  @Test
  public void shouldSupportDataKeyThatEqualsMetadataKey() {
    // Given:
    final byte[] valBytes = new byte[]{0x1};
    client.execute(this.table.insert(0, ColumnName.METADATA_KEY, valBytes, CURRENT_TS));

    // When:
    final byte[] val = this.table.get(0, ColumnName.METADATA_KEY, MIN_VALID_TS);

    // Then:
    MatcherAssert.assertThat(val, Matchers.is(valBytes));
  }

}