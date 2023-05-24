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

package dev.responsive.db;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.utility.DockerImageName;

public class CassandraClientIntegrationTest {

  @Rule public CassandraContainer<?> cassandra = new CassandraContainer<>(
      DockerImageName.parse("cassandra:4.1.0")
  ).withInitScript("CassandraDockerInit.cql");
  @Rule public TestName name = new TestName();

  private CqlSession session;
  private CassandraClient client;

  @Before
  public void before() {
    session = CqlSession.builder()
        .addContactPoint(cassandra.getContactPoint())
        .withLocalDatacenter(cassandra.getLocalDatacenter())
        .withKeyspace("responsive_clients") // NOTE: this keyspace is expected to exist
        .build();
    client = new CassandraClient(session);

    client.createDataTable(name.getMethodName());
    client.prepareStatements(name.getMethodName());
  }

  @Test
  public void shouldReturnAllKeysInLexicalOrder() {
    // Given:
    final String table = name.getMethodName();
    final List<BoundStatement> inserts = List.of(
        client.insertData(table, 0, Bytes.wrap(new byte[]{0x0, 0x1}), new byte[]{0x1}),
        client.insertData(table, 0, Bytes.wrap(new byte[]{0x1, 0x0}), new byte[]{0x1}),
        client.insertData(table, 0, Bytes.wrap(new byte[]{0x2, 0x0}), new byte[]{0x1}),
        client.insertData(table, 0, Bytes.wrap(new byte[]{0x2, 0x2}), new byte[]{0x1}),
        client.insertData(table, 0, Bytes.wrap(new byte[]{0x1, 0x1}), new byte[]{0x1}),
        client.insertData(table, 0, Bytes.wrap(new byte[]{0x0, 0x2}), new byte[]{0x1}),
        client.insertData(table, 0, Bytes.wrap(new byte[]{0x2}), new byte[]{0x1}),
        client.insertData(table, 0, Bytes.wrap(new byte[]{0x0}), new byte[]{0x1})
    );
    inserts.forEach(client::execute);

    // When:
    final KeyValueIterator<Bytes, byte[]> all = client.all(table, 0);

    // Then:
    Bytes old = all.next().key;
    while (all.hasNext()) {
      final Bytes next = all.next().key;
      Assert.assertThat(next.compareTo(old), Matchers.greaterThan(0));
      old = next;
    }
  }

  @Test
  public void shouldReturnRangeKeysInLexicalOrder() {
    // Given:
    final String table = name.getMethodName();
    final List<BoundStatement> inserts = List.of(
        client.insertData(table, 0, Bytes.wrap(new byte[]{0x0, 0x1}), new byte[]{0x1}),
        client.insertData(table, 0, Bytes.wrap(new byte[]{0x1, 0x0}), new byte[]{0x1}),
        client.insertData(table, 0, Bytes.wrap(new byte[]{0x2, 0x0}), new byte[]{0x1}),
        client.insertData(table, 0, Bytes.wrap(new byte[]{0x2, 0x2}), new byte[]{0x1}),
        client.insertData(table, 0, Bytes.wrap(new byte[]{0x1, 0x1}), new byte[]{0x1}),
        client.insertData(table, 0, Bytes.wrap(new byte[]{0x0, 0x2}), new byte[]{0x1}),
        client.insertData(table, 0, Bytes.wrap(new byte[]{0x2}), new byte[]{0x1}),
        client.insertData(table, 0, Bytes.wrap(new byte[]{0x0}), new byte[]{0x1})
    );
    inserts.forEach(client::execute);

    // When:
    final KeyValueIterator<Bytes, byte[]> range = client.range(
        table,
        0,
        Bytes.wrap(new byte[]{0x0, 0x1}),
        Bytes.wrap(new byte[]{0x0, 0x3}));

    // Then:
    final List<Bytes> keys = new ArrayList<>();
    range.forEachRemaining(kv -> keys.add(kv.key));
    MatcherAssert.assertThat(keys, Matchers.hasSize(2));
    MatcherAssert.assertThat(keys, Matchers.hasItems(
        Bytes.wrap(new byte[]{0x0, 0x1}),
        Bytes.wrap(new byte[]{0x0, 0x2})
    ));
  }

}