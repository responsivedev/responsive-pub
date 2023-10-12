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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.containsStringIgnoringCase;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import com.datastax.oss.driver.api.core.CqlSession;
import dev.responsive.kafka.api.config.ResponsiveConfig;
import dev.responsive.kafka.api.stores.ResponsiveKeyValueParams;
import dev.responsive.kafka.internal.db.partitioning.SubPartitioner;
import dev.responsive.kafka.testutils.ResponsiveConfigParam;
import dev.responsive.kafka.testutils.ResponsiveExtension;
import java.time.Duration;
import java.util.Map;
import org.apache.kafka.common.utils.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.CassandraContainer;

@ExtendWith(ResponsiveExtension.class)
class CassandraFactTableIntegrationTest {

  private static final long CURRENT_TIME = 100L;
  private static final long MIN_VALID_TS = 0L;

  private String storeName; // ie the "kafkaName", NOT the "cassandraName"
  private ResponsiveKeyValueParams params;
  private CassandraClient client;
  private CqlSession session;

  @BeforeEach
  public void before(
      final TestInfo info,
      @ResponsiveConfigParam final Map<String, Object> responsiveProps,
      final CassandraContainer<?> cassandra
  ) {
    String name = info.getTestMethod().orElseThrow().getName();
    storeName = name + "store";

    session = CqlSession.builder()
        .addContactPoint(cassandra.getContactPoint())
        .withLocalDatacenter(cassandra.getLocalDatacenter())
        .withKeyspace("responsive_clients") // NOTE: this keyspace is expected to exist
        .build();
    client = new CassandraClient(session, ResponsiveConfig.responsiveConfig(responsiveProps));
  }

  @Test
  public void shouldInitializeWithCorrectMetadata() throws Exception {
    // Given:
    params = ResponsiveKeyValueParams.fact(storeName);
    final String tableName = params.name().remoteName();
    final RemoteKVTable schema = client
        .factFactory()
        .create(CassandraTableSpecFactory.fromKVParams(params));

    // When:
    final var token = schema.init(SubPartitioner.NO_SUBPARTITIONS, 1);
    schema.init(SubPartitioner.NO_SUBPARTITIONS, 2);
    client.execute(schema.setOffset(2, 10));
    final MetadataRow metadata1 = schema.metadata(1);
    final MetadataRow metadata2 = schema.metadata(2);

    // Then:
    assertThat(token, instanceOf(FactWriterFactory.class));
    assertThat(metadata1.offset, is(-1L));
    assertThat(metadata1.epoch, is(-1L));
    assertThat(metadata2.offset, is(10L));
    assertThat(metadata2.epoch, is(-1L));

    // ensure it uses a separate table for metadata
    final var table = session.getMetadata()
        .getKeyspace(session.getKeyspace().get())
        .get()
        .getTable(tableName + "_md")
        .get();
    assertThat(table.describe(false), containsStringIgnoringCase(tableName + "_md"));
  }

  @SuppressWarnings("OptionalGetWithoutIsPresent")
  @Test
  public void shouldSetTtlAndCorrectTwcsOptions() throws Exception {
    // Given:
    final var ttl = Duration.ofDays(30);
    params = ResponsiveKeyValueParams.fact(storeName).withTimeToLive(ttl);
    final String tableName = params.name().remoteName();

    // When:
    final RemoteKVTable schema = client
        .factFactory()
        .create(CassandraTableSpecFactory.fromKVParams(params));

    // Then:
    final var table = session.getMetadata()
        .getKeyspace(session.getKeyspace().get())
        .get()
        .getTable(tableName)
        .get();
    final String describe = table.describe(false);
    assertThat(describe,
        containsString(
            "'class':'org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy',"
                + "'compaction_window_size':'" + (ttl.toMinutes() / 20) + "',"
                + "'compaction_window_unit':'MINUTES'")
    );
    assertThat(describe, containsString("default_time_to_live = " + (int) ttl.toSeconds()));
  }

  @SuppressWarnings("OptionalGetWithoutIsPresent")
  @Test
  public void shouldUseTwcsWithoutTtl() throws Exception {
    // Given:
    params = ResponsiveKeyValueParams.fact(storeName);
    final String tableName = params.name().remoteName();

    // When:
    client.factFactory()
        .create(CassandraTableSpecFactory.fromKVParams(params));

    // Then:
    final var table = session.getMetadata()
        .getKeyspace(session.getKeyspace().get())
        .get()
        .getTable(tableName)
        .get();
    final String describe = table.describe(false);
    assertThat(describe,
        containsString("'org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy'"));
  }

  @Test
  public void shouldInsertAndDelete() throws Exception {
    // Given:
    params = ResponsiveKeyValueParams.fact(storeName);
    final RemoteKVTable table = client
        .factFactory()
        .create(CassandraTableSpecFactory.fromKVParams(params));

    table.init(SubPartitioner.NO_SUBPARTITIONS, 1);

    final Bytes key = Bytes.wrap(new byte[]{0});
    final byte[] val = new byte[]{1};

    // When:
    client.execute(table.insert(1, key, val, CURRENT_TIME));
    final byte[] valAt0 = table.get(1, key, MIN_VALID_TS);
    client.execute(table.delete(1, key));
    final byte[] valAt1 = table.get(1, key, MIN_VALID_TS);

    // Then
    assertThat(valAt0, is(val));
    assertThat(valAt1, nullValue());
  }

  @Test
  public void shouldRespectSemanticTTL() throws Exception {
    // Given:
    params = ResponsiveKeyValueParams.fact(storeName);
    final String tableName = params.name().remoteName();

    final RemoteKVTable table = client
        .factFactory()
        .create(CassandraTableSpecFactory.fromKVParams(params));

    table.init(SubPartitioner.NO_SUBPARTITIONS, 1);

    final Bytes key = Bytes.wrap(new byte[]{0});
    final byte[] val = new byte[]{1};

    // When:
    client.execute(table.insert(1, key, val, CURRENT_TIME));
    final byte[] valid = table.get(1, key, MIN_VALID_TS);
    final byte[] expired = table.get(1, key, CURRENT_TIME + 100L);

    // Then
    assertThat(valid, is(val));
    assertThat(expired, nullValue());
  }

}