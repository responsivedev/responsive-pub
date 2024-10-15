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
import static dev.responsive.kafka.testutils.IntegrationTestUtils.serializedKey;
import static dev.responsive.kafka.testutils.IntegrationTestUtils.serializedValue;
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
import dev.responsive.kafka.api.stores.TtlProvider.TtlDuration;
import dev.responsive.kafka.internal.stores.TtlResolver;
import dev.responsive.kafka.testutils.ResponsiveConfigParam;
import dev.responsive.kafka.testutils.ResponsiveExtension;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.CassandraContainer;

@ExtendWith(ResponsiveExtension.class)
class CassandraFactTableIntegrationTest {

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
    storeName = name + "-store";

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
    final var ignored = schema.init(1);
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
    final long minValidTs = 0L;

    // When:
    client.execute(table.insert(1, key, val, 0L));
    final byte[] valAt0 = table.get(1, key, minValidTs);
    client.execute(table.delete(1, key));
    final byte[] valAt1 = table.get(1, key, minValidTs);

    // Then
    assertThat(valAt0, is(val));
    assertThat(valAt1, nullValue());
  }

  @SuppressWarnings("OptionalGetWithoutIsPresent")
  @Test
  public void shouldRespectSemanticDefaultOnlyTtl() throws Exception {
    // Given:
    final var defaultTtl = Duration.ofMinutes(30);
    final var ttlProvider = TtlProvider.<String, String>withDefault(defaultTtl);
    params = ResponsiveKeyValueParams.fact(storeName).withTtlProvider(ttlProvider);
    final String tableName = params.name().tableName();

    // When:
    client.factFactory().create(RemoteTableSpecFactory.fromKVParams(
        params,
        defaultPartitioner(),
        Optional.of(new TtlResolver<>(false, "changelog-ignored", ttlProvider))
    ));

    // Then:
    final var table = session.getMetadata()
        .getKeyspace(session.getKeyspace().get())
        .get()
        .getTable(tableName)
        .get();
    final String describe = table.describe(false);

    assertThat(describe, containsString("default_time_to_live = " + (int) defaultTtl.toSeconds()));
  }

  @Test
  public void shouldRespectSemanticKeyBasedTtl() throws Exception {
    // Given:
    final var defaultTtl = Duration.ofMinutes(30);
    final Function<String, Optional<TtlDuration>> ttlForKey = k -> {
      if (k.equals("KEEP_FOREVER")) {
        return Optional.of(TtlDuration.noTtl());
      } else if (k.endsWith("DEFAULT_TTL")) {
        return Optional.empty();
      } else {
        final long ttlMinutes = Long.parseLong(k);
        return Optional.of(TtlDuration.of(Duration.ofMinutes(ttlMinutes)));
      }
    };
    final TtlProvider<String, String> ttlProvider = TtlProvider
        .<String, String>withDefault(defaultTtl)
        .fromKey(ttlForKey, Serdes.String());
    params = ResponsiveKeyValueParams.fact(storeName).withTtlProvider(ttlProvider);

    final var table = client.factFactory().create(RemoteTableSpecFactory.fromKVParams(
        params,
        defaultPartitioner(),
        Optional.of(new TtlResolver<>(false, "changelog-ignored", ttlProvider))
    ));

    table.init(1);

    final Bytes noTtlKey = serializedKey("KEEP_FOREVER");
    final Bytes defaultTtlKey = serializedKey("DEFAULT_TTL"); // default is 30min
    final Bytes tenMinTtlKey = serializedKey(String.valueOf(10L));
    final Bytes fiftyMinTtlKey = serializedKey(String.valueOf(50L));

    final byte[] val = new byte[]{1};

    // When:
    final long insertTimeMs = 0L;
    client.execute(table.insert(1, noTtlKey, val, insertTimeMs));
    client.execute(table.insert(1, defaultTtlKey, val, insertTimeMs));
    client.execute(table.insert(1, tenMinTtlKey, val, insertTimeMs));
    client.execute(table.insert(1, fiftyMinTtlKey, val, insertTimeMs));

    // Then
    long lookupTime = Duration.ofMinutes(11).toMillis();
    assertThat(table.get(1, noTtlKey, lookupTime), is(val));
    assertThat(table.get(1, defaultTtlKey, lookupTime), is(val));
    assertThat(table.get(1, tenMinTtlKey, lookupTime), nullValue()); // expired
    assertThat(table.get(1, fiftyMinTtlKey, lookupTime), is(val));

    lookupTime = Duration.ofMinutes(31).toMillis();
    assertThat(table.get(1, noTtlKey, lookupTime), is(val));
    assertThat(table.get(1, defaultTtlKey, lookupTime), nullValue());    // expired
    assertThat(table.get(1, tenMinTtlKey, lookupTime), nullValue()); // expired
    assertThat(table.get(1, fiftyMinTtlKey, lookupTime), is(val));

    lookupTime = Duration.ofMinutes(51).toMillis();
    assertThat(table.get(1, noTtlKey, lookupTime), is(val));
    assertThat(table.get(1, defaultTtlKey, lookupTime), nullValue());    // expired
    assertThat(table.get(1, tenMinTtlKey, lookupTime), nullValue()); // expired
    assertThat(table.get(1, fiftyMinTtlKey, lookupTime), nullValue());   // expired
  }

  @Test
  public void shouldRespectSemanticKeyValueBasedTtl() throws Exception {
    // Given:
    final var defaultTtl = Duration.ofMinutes(30);
    final BiFunction<String, String, Optional<TtlDuration>> ttlForKeyAndValue = (k, v) -> {
      if (k.equals("10_MINUTE_RETENTION")) {
        return Optional.of(TtlDuration.of(Duration.ofMinutes(10)));
      } else {
        if (v.equals("DEFAULT")) {
          return Optional.empty();
        } else if (v.equals("NO_TTL")) {
          return Optional.of(TtlDuration.noTtl());
        } else {
          return Optional.of(TtlDuration.of(Duration.ofMinutes(Long.parseLong(v))));
        }
      }
    };
    final TtlProvider<String, String> ttlProvider = TtlProvider
        .<String, String>withDefault(defaultTtl)
        .fromKeyAndValue(ttlForKeyAndValue, Serdes.String(), Serdes.String());
    params = ResponsiveKeyValueParams.fact(storeName).withTtlProvider(ttlProvider);


    final var table = client.factFactory().create(RemoteTableSpecFactory.fromKVParams(
        params,
        defaultPartitioner(),
        Optional.of(new TtlResolver<>(false, "changelog-ignored", ttlProvider))
    ));

    table.init(1);

    final Bytes tenMinTtlKey = serializedKey("10_MINUTE_RETENTION");
    final Bytes defaultTtlKey = serializedKey("DEFAULT_30_MIN_RETENTION");
    final Bytes noTtlKey = serializedKey("NO_TTL");
    final Bytes twoMinTtlKey = serializedKey("2_MINUTE_RETENTION");
    final Bytes fiftyMinTtlKey = serializedKey("50_MINUTE_RETENTION");

    final byte[] defaultTtlValue = serializedValue("DEFAULT"); // default is 30min
    final byte[] noTtlValue = serializedValue("NO_TTL");
    final byte[] twoMinTtlValue = serializedValue(String.valueOf(2));
    final byte[] fiftyMinTtlValue = serializedValue(String.valueOf(50));

    final byte[] val = new byte[]{1};

    // When
    long insertTimeMs = 0L;
    client.execute(table.insert(1, tenMinTtlKey, val, insertTimeMs));
    client.execute(table.insert(1, defaultTtlKey, defaultTtlValue, insertTimeMs));
    client.execute(table.insert(1, noTtlKey, noTtlValue, insertTimeMs));
    client.execute(table.insert(1, twoMinTtlKey, twoMinTtlValue, insertTimeMs));
    client.execute(table.insert(1, fiftyMinTtlKey, fiftyMinTtlValue, insertTimeMs));

    // Then:
    long lookupTime = Duration.ofMinutes(3).toMillis();
    assertThat(table.get(1, tenMinTtlKey, lookupTime), is(val));
    assertThat(table.get(1, defaultTtlKey, lookupTime), is(defaultTtlValue));
    assertThat(table.get(1, noTtlKey, lookupTime), is(noTtlValue));
    assertThat(table.get(1, twoMinTtlKey, lookupTime), nullValue());            // expired
    assertThat(table.get(1, fiftyMinTtlKey, lookupTime), is(fiftyMinTtlValue));

    lookupTime = Duration.ofMinutes(11).toMillis();
    assertThat(table.get(1, tenMinTtlKey, lookupTime), nullValue());            // expired
    assertThat(table.get(1, defaultTtlKey, lookupTime), is(defaultTtlValue));
    assertThat(table.get(1, noTtlKey, lookupTime), is(noTtlValue));
    assertThat(table.get(1, twoMinTtlKey, lookupTime), nullValue());            // expired
    assertThat(table.get(1, fiftyMinTtlKey, lookupTime), is(fiftyMinTtlValue));

    lookupTime = Duration.ofMinutes(31).toMillis();
    assertThat(table.get(1, tenMinTtlKey, lookupTime), nullValue());            // expired
    assertThat(table.get(1, defaultTtlKey, lookupTime), nullValue());           // expired
    assertThat(table.get(1, noTtlKey, lookupTime), is(noTtlValue));
    assertThat(table.get(1, twoMinTtlKey, lookupTime), nullValue());            // expired
    assertThat(table.get(1, fiftyMinTtlKey, lookupTime), is(fiftyMinTtlValue));

    lookupTime = Duration.ofMinutes(51).toMillis();
    assertThat(table.get(1, tenMinTtlKey, lookupTime), nullValue());            // expired
    assertThat(table.get(1, defaultTtlKey, lookupTime), nullValue());           // expired
    assertThat(table.get(1, noTtlKey, lookupTime), is(noTtlValue));
    assertThat(table.get(1, twoMinTtlKey, lookupTime), nullValue());            // expired
    assertThat(table.get(1, fiftyMinTtlKey, lookupTime), nullValue());          // expired
  }

  @Test
  public void shouldRespectOverridesWithValueBasedTtl() throws Exception {
    // Given:
    final var defaultTtl = Duration.ofMinutes(30);
    final Function<String, Optional<TtlDuration>> ttlForValue = v -> {
      if (v.equals("DEFAULT")) {
        return Optional.empty();
      } else if (v.equals("NO_TTL")) {
        return Optional.of(TtlDuration.noTtl());
      } else {
        return Optional.of(TtlDuration.of(Duration.ofMinutes(Long.parseLong(v))));
      }
    };
    final TtlProvider<String, String> ttlProvider = TtlProvider
        .<String, String>withDefault(defaultTtl)
        .fromValue(ttlForValue, Serdes.String());
    params = ResponsiveKeyValueParams.fact(storeName).withTtlProvider(ttlProvider);


    final var table = client.factFactory().create(RemoteTableSpecFactory.fromKVParams(
        params,
        defaultPartitioner(),
        Optional.of(new TtlResolver<>(false, "changelog-ignored", ttlProvider))
    ));

    table.init(1);


    final Bytes key = serializedKey("ignored");

    final byte[] defaultTtlValue = serializedValue("DEFAULT"); // default is 30min
    final byte[] noTtlValue = serializedValue("NO_TTL");
    final byte[] threeMinTtlValue = serializedValue(String.valueOf(3));
    final byte[] tenMinTtlValue = serializedValue(String.valueOf(10));

    // When
    long currentTime = 0L;
    // first record set to expire at 3min
    client.execute(table.insert(1, key, threeMinTtlValue, currentTime));

    // Then:
    currentTime = Duration.ofMinutes(4).toMillis();
    assertThat(table.get(1, key, currentTime), nullValue()); // expired

    // insert new record with 3min ttl -- now set to expire at 10min
    currentTime = Duration.ofMinutes(7).toMillis();
    client.execute(table.insert(1, key, threeMinTtlValue, currentTime));

    // override with 10min ttl -- now set to expire at 18min
    currentTime = Duration.ofMinutes(8).toMillis();
    client.execute(table.insert(1, key, tenMinTtlValue, currentTime));

    // record should still exist after 10min
    currentTime = Duration.ofMinutes(11).toMillis();
    assertThat(table.get(1, key, currentTime), is(tenMinTtlValue));

    // override with default ttl (30min) -- now set to expire at 45min
    currentTime = Duration.ofMinutes(15).toMillis();
    client.execute(table.insert(1, key, defaultTtlValue, currentTime));

    // record should still exist after 18min
    currentTime = Duration.ofMinutes(20).toMillis();
    assertThat(table.get(1, key, currentTime), is(defaultTtlValue));

    // override with no ttl -- now set to never expire
    currentTime = Duration.ofMinutes(30).toMillis();
    client.execute(table.insert(1, key, noTtlValue, currentTime));

    // record should still exist after 45min
    currentTime = Duration.ofMinutes(50).toMillis();
    assertThat(table.get(1, key, currentTime), is(noTtlValue));
  }


}