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

import static dev.responsive.kafka.internal.db.partitioning.TablePartitioner.defaultPartitioner;
import static dev.responsive.kafka.internal.stores.TtlResolver.NO_TTL;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.containsStringIgnoringCase;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import dev.responsive.kafka.api.config.ResponsiveConfig;
import dev.responsive.kafka.api.stores.ResponsiveKeyValueParams;
import dev.responsive.kafka.api.stores.TtlProvider;
import dev.responsive.kafka.internal.stores.TtlResolver;
import dev.responsive.kafka.testutils.ResponsiveConfigParam;
import dev.responsive.kafka.testutils.ResponsiveExtension;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.utils.Bytes;
import org.hamcrest.Matchers;
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
        .withKeyspace("responsive_itests") // NOTE: this keyspace is expected to exist
        .build();
    client = new CassandraClient(session, ResponsiveConfig.responsiveConfig(responsiveProps));
  }

  @Test
  public void shouldInitializeWithCorrectMetadata() throws Exception {
    // Given:
    params = ResponsiveKeyValueParams.fact(storeName);
    final String tableName = params.name().tableName();
    final CassandraFactTable schema = (CassandraFactTable) client
        .factFactory()
        .create(RemoteTableSpecFactory.fromKVParams(params, defaultPartitioner(), NO_TTL));

    // When:
    final var token = schema.init(1);
    schema.init(2);
    client.execute(schema.setOffset(2, 10));
    final long offset1 = schema.fetchOffset(1);
    final long offset2 = schema.fetchOffset(2);

    // Then:
    assertThat(offset1, is(-1L));
    assertThat(offset2, is(10L));

    // ensure it uses a separate table for metadata
    final var table = session.getMetadata()
        .getKeyspace(session.getKeyspace().get())
        .get()
        .getTable(tableName + "_md")
        .get();
    assertThat(table.describe(false), containsStringIgnoringCase(tableName + "_md"));
  }

  @Test
  public void shouldInsertAndDelete() throws Exception {
    // Given:
    params = ResponsiveKeyValueParams.fact(storeName);
    final RemoteKVTable<BoundStatement> table = client
        .factFactory()
        .create(RemoteTableSpecFactory.fromKVParams(params, defaultPartitioner(), NO_TTL));

    table.init(1);

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

  @SuppressWarnings("OptionalGetWithoutIsPresent")
  @Test
  public void shouldRespectSemanticDefaultOnlyTtl() throws Exception {
    // Given:
    final long ttlMs = 100L;
    final var ttlProvider = TtlProvider.<String, String>withDefault(Duration.ofMillis(ttlMs));
    params = ResponsiveKeyValueParams.fact(storeName).withTtlProvider(ttlProvider);
    final String tableName = params.name().tableName();

    final var table = client.factFactory().create(RemoteTableSpecFactory.fromKVParams(
        params,
        defaultPartitioner(),
        Optional.of(new TtlResolver<>(false, "changelog-ignored", ttlProvider))
    ));

    // When:
    final long insertTimeMs = 0L;
    client.execute(
        table.insert(0, Bytes.wrap(new byte[]{0x0, 0x1}), new byte[]{0x1}, insertTimeMs));

    final String tableDescription = session.getMetadata()
        .getKeyspace(session.getKeyspace().get())
        .get()
        .getTable(tableName)
        .get()
        .describe(false);

    // Then:
    final int ttlSeconds = (int) TimeUnit.MILLISECONDS.toSeconds(ttlMs);
    assertThat(tableDescription, containsString("default_time_to_live = " + ttlSeconds));

    final long lookupTimeValid = ttlMs - 1L;
    final byte[] valid = table.get(0, Bytes.wrap(new byte[]{0x0, 0x1}), lookupTimeValid);

    final long lookupTimeExpired = ttlMs + 1L;
    final byte[] expired = table.get(0, Bytes.wrap(new byte[]{0x0, 0x1}), lookupTimeExpired);

    assertThat(valid, Matchers.is(new byte[]{0x1}));
    assertThat(expired, Matchers.nullValue());
  }

}