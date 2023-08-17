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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.containsStringIgnoringCase;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import com.datastax.oss.driver.api.core.CqlSession;
import dev.responsive.db.partitioning.SubPartitioner;
import dev.responsive.kafka.config.ResponsiveConfig;
import dev.responsive.kafka.store.SchemaType;
import dev.responsive.utils.ResponsiveConfigParam;
import dev.responsive.utils.ResponsiveExtension;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.utils.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.CassandraContainer;

@ExtendWith(ResponsiveExtension.class)
class CassandraFactSchemaIntegrationTest {

  private String storeName;
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
    client = new CassandraClient(session, new ResponsiveConfig(responsiveProps));
  }

  @Test
  public void shouldInitializeWithCorrectMetadata() {
    // Given:
    final RemoteKeyValueSchema schema = client.kvSchema(SchemaType.FACT);
    schema.create(storeName, Optional.empty());
    schema.prepare(storeName);

    // When:
    final var token = schema.init(storeName, SubPartitioner.NO_SUBPARTITIONS, 1);
    schema.init(storeName, SubPartitioner.NO_SUBPARTITIONS, 2);
    client.execute(schema.setOffset(storeName, 2, 10));
    final MetadataRow metadata1 = schema.metadata(storeName, 1);
    final MetadataRow metadata2 = schema.metadata(storeName, 2);

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
        .getTable(CassandraFactSchema.metadataTable(storeName))
        .get();
    assertThat(
        table.describe(false),
        containsStringIgnoringCase(CassandraFactSchema.metadataTable(storeName)));
  }

  @SuppressWarnings("OptionalGetWithoutIsPresent")
  @Test
  public void shouldSetTtlAndCorrectTwcsOptions() {
    // Given:
    final RemoteKeyValueSchema schema = client.kvSchema(SchemaType.FACT);
    final var ttl = Duration.ofDays(30);

    // When:
    schema.create(storeName, Optional.of(ttl));

    // Then:
    final var table = session.getMetadata()
        .getKeyspace(session.getKeyspace().get())
        .get()
        .getTable(storeName)
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
  public void shouldUseTwcsWithoutTtl() {
    // Given:
    final RemoteKeyValueSchema schema = client.kvSchema(SchemaType.FACT);

    // When:
    schema.create(storeName, Optional.empty());

    // Then:
    final var table = session.getMetadata()
        .getKeyspace(session.getKeyspace().get())
        .get()
        .getTable(storeName)
        .get();
    final String describe = table.describe(false);
    assertThat(describe,
        containsString("'org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy'"));
  }

  @Test
  public void shouldInsertAndDelete() {
    // Given:
    final RemoteKeyValueSchema schema = client.kvSchema(SchemaType.FACT);
    schema.create(storeName, Optional.empty());
    schema.prepare(storeName);
    schema.init(storeName, SubPartitioner.NO_SUBPARTITIONS, 1);

    final Bytes key = Bytes.wrap(new byte[]{0});
    final byte[] val = new byte[]{1};

    // When:
    client.execute(schema.insert(storeName, 1, key, val));
    final byte[] valAt0 = schema.get(storeName, 1, key);
    client.execute(schema.delete(storeName, 1, key));
    final byte[] valAt1 = schema.get(storeName, 1, key);

    // Then
    assertThat(valAt0, is(val));
    assertThat(valAt1, nullValue());
  }

}