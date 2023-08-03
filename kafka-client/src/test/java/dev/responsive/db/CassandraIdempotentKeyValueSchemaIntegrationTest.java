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
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import com.datastax.oss.driver.api.core.CqlSession;
import dev.responsive.db.partitioning.SubPartitioner;
import dev.responsive.kafka.config.ResponsiveConfig;
import dev.responsive.kafka.store.SchemaType;
import dev.responsive.utils.ResponsiveConfigParam;
import dev.responsive.utils.ResponsiveExtension;
import java.util.Map;
import org.apache.kafka.common.utils.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.CassandraContainer;

@ExtendWith(ResponsiveExtension.class)
class CassandraIdempotentKeyValueSchemaIntegrationTest {

  private String storeName;
  private CassandraClient client;

  @BeforeEach
  public void before(
      final TestInfo info,
      @ResponsiveConfigParam final Map<String, Object> responsiveProps,
      final CassandraContainer<?> cassandra
  ) {
    String name = info.getTestMethod().orElseThrow().getName();
    storeName = name + "store";

    final CqlSession session = CqlSession.builder()
        .addContactPoint(cassandra.getContactPoint())
        .withLocalDatacenter(cassandra.getLocalDatacenter())
        .withKeyspace("responsive_clients") // NOTE: this keyspace is expected to exist
        .build();
    client = new CassandraClient(session, new ResponsiveConfig(responsiveProps));
  }

  @Test
  public void shouldInitializeWithCorrectMetadata() {
    // Given:
    final RemoteKeyValueSchema schema = client.kvSchema(SchemaType.IDEMPOTENT);
    schema.create(storeName);
    schema.prepare(storeName);

    // When:
    final var token = schema.init(storeName, SubPartitioner.NO_SUBPARTITIONS, 1);
    schema.init(storeName, SubPartitioner.NO_SUBPARTITIONS, 2);
    client.execute(schema.setOffset(storeName, new NoOpFencingToken(), 2, 10));
    final MetadataRow metadata1 = schema.metadata(storeName, 1);
    final MetadataRow metadata2 = schema.metadata(storeName, 2);

    // Then:
    assertThat(token, instanceOf(NoOpFencingToken.class));
    assertThat(metadata1.offset, is(-1L));
    assertThat(metadata1.epoch, is(-1L));
    assertThat(metadata2.offset, is(10L));
    assertThat(metadata2.epoch, is(-1L));
  }

  @Test
  public void shouldInsertAndDelete() {
    // Given:
    final RemoteKeyValueSchema schema = client.kvSchema(SchemaType.IDEMPOTENT);
    schema.create(storeName);
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