/*
 * Copyright 2024 Responsive Computing, Inc.
 *
 * This source code is licensed under the Responsive Business Source License Agreement v1.0
 * available at:
 *
 * https://www.responsive.dev/legal/responsive-bsl-10
 *
 * This software requires a valid Commercial License Key for production use. Trial and commercial
 * licenses can be obtained at https://www.responsive.dev
 */

package dev.responsive.kafka.internal.db;

import static dev.responsive.kafka.internal.db.partitioning.TablePartitioner.defaultPartitioner;
import static dev.responsive.kafka.internal.stores.TtlResolver.NO_TTL;
import static dev.responsive.kafka.testutils.IntegrationTestUtils.defaultOnlyTtl;
import static dev.responsive.kafka.testutils.SerdeUtils.serializedKey;
import static dev.responsive.kafka.testutils.SerdeUtils.serializedValue;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.containsStringIgnoringCase;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import dev.responsive.kafka.api.config.ResponsiveConfig;
import dev.responsive.kafka.api.config.StorageBackend;
import dev.responsive.kafka.api.stores.ResponsiveKeyValueParams;
import dev.responsive.kafka.api.stores.TtlProvider;
import dev.responsive.kafka.api.stores.TtlProvider.TtlDuration;
import dev.responsive.kafka.internal.stores.TtlResolver;
import dev.responsive.kafka.internal.utils.StateDeserializer;
import dev.responsive.kafka.testutils.ResponsiveConfigParam;
import dev.responsive.kafka.testutils.ResponsiveExtension;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.Bytes;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.containers.CassandraContainer;

class CassandraFactTableIntegrationTest {

  @RegisterExtension
  static ResponsiveExtension EXTENSION = new ResponsiveExtension(StorageBackend.CASSANDRA);

  private String storeName; // ie the "kafkaName", NOT the "cassandraName"
  private ResponsiveKeyValueParams params;
  private CassandraClient client;
  private CqlSession session;
  private ResponsiveConfig config;

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
    config = ResponsiveConfig.responsiveConfig(responsiveProps);
  }

  @Test
  public void shouldInitializeWithCorrectMetadata() throws Exception {
    // Given:
    params = ResponsiveKeyValueParams.fact(storeName);
    final String tableName = params.name().tableName();
    final CassandraFactTable schema = (CassandraFactTable) client
        .factFactory()
        .create(RemoteTableSpecFactory.fromKVParams(params, defaultPartitioner(), NO_TTL, config));

    // When:
    final var token = schema.init(1);
    schema.init(2);
    client.execute(schema.setOffset(2, 10));
    final long offset1 = schema.lastWrittenOffset(1);
    final long offset2 = schema.lastWrittenOffset(2);

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
        .create(RemoteTableSpecFactory.fromKVParams(params, defaultPartitioner(), NO_TTL, config));

    table.init(1);

    final Bytes key = Bytes.wrap(new byte[]{0});
    final byte[] val = new byte[]{1};

    // When:
    client.execute(table.insert(1, key, val, 0L));
    final byte[] valAt0 = table.get(1, key, 0L);
    client.execute(table.delete(1, key));
    final byte[] valAt1 = table.get(1, key, 0L);

    // Then
    assertThat(valAt0, is(val));
    assertThat(valAt1, nullValue());
  }

  @SuppressWarnings("OptionalGetWithoutIsPresent")
  @Test
  public void shouldConfigureDefaultTtl() throws Exception {
    // Given:
    final long ttlMs = 100L;
    final TtlProvider<?, ?> ttlProvider = TtlProvider.withDefault(Duration.ofMillis(ttlMs));
    final ResponsiveKeyValueParams params =
        ResponsiveKeyValueParams.fact(storeName).withTtlProvider(ttlProvider);
    final String tableName = params.name().tableName();

    // When:
    client.factFactory().create(RemoteTableSpecFactory.fromKVParams(
        params,
        defaultPartitioner(),
        defaultOnlyTtl(Duration.ofMillis(ttlMs)),
        config
    ));

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
  public void shouldRespectSemanticDefaultOnlyTtl() throws Exception {
    // Given:
    final long ttlMs = MINUTES.toMillis(100);
    final var ttlProvider = TtlProvider.<String, String>withDefault(Duration.ofMillis(ttlMs));
    params = ResponsiveKeyValueParams.fact(storeName).withTtlProvider(ttlProvider);

    final var table = client.factFactory().create(RemoteTableSpecFactory.fromKVParams(
        params,
        defaultPartitioner(),
        defaultOnlyTtl(Duration.ofMillis(ttlMs)),
        config
    ));

    final long insertTimeMs = 0L;
    client.execute(
        table.insert(0, Bytes.wrap(new byte[]{0x0, 0x1}), new byte[]{0x1}, insertTimeMs));

    // When:
    final long lookupTimeValid = ttlMs - 1L;
    final byte[] valid = table.get(0, Bytes.wrap(new byte[]{0x0, 0x1}), lookupTimeValid);

    final long lookupTimeExpired = ttlMs + 1L;
    final byte[] expired = table.get(0, Bytes.wrap(new byte[]{0x0, 0x1}), lookupTimeExpired);

    // Then:
    assertThat(valid, Matchers.is(new byte[]{0x1}));
    assertThat(expired, Matchers.nullValue());
  }

  @Test
  public void shouldRespectSemanticKeyBasedTtl() throws Exception {
    // Given:
    final var defaultTtl = Duration.ofMinutes(30);
    final Function<String, Optional<TtlDuration>> ttlForKey = k -> {
      if (k.equals("KEEP_FOREVER")) {
        return Optional.of(TtlDuration.infinite());
      } else if (k.endsWith("DEFAULT_TTL")) {
        return Optional.empty();
      } else {
        final long ttlMinutes = Long.parseLong(k);
        return Optional.of(TtlDuration.of(Duration.ofMinutes(ttlMinutes)));
      }
    };
    final TtlProvider<String, String> ttlProvider = TtlProvider
        .<String, String>withDefault(defaultTtl)
        .fromKey(ttlForKey);
    params = ResponsiveKeyValueParams.fact(storeName).withTtlProvider(ttlProvider);

    final var table = client.factFactory().create(RemoteTableSpecFactory.fromKVParams(
        params,
        defaultPartitioner(),
        Optional.of(new TtlResolver<>(
            new StateDeserializer<>("ignored", new StringDeserializer(), new StringDeserializer()),
            ttlProvider)),
        config
    ));

    table.init(1);

    final Bytes noTtlKey = serializedKey("KEEP_FOREVER");
    final Bytes defaultTtlKey = serializedKey("DEFAULT_TTL"); // default is 30ms
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
    long lookupTimeMs = Duration.ofMinutes(11).toMillis();
    assertThat(table.get(1, noTtlKey, lookupTimeMs), is(val));
    assertThat(table.get(1, defaultTtlKey, lookupTimeMs), is(val));
    assertThat(table.get(1, tenMinTtlKey, lookupTimeMs), nullValue()); // expired
    assertThat(table.get(1, fiftyMinTtlKey, lookupTimeMs), is(val));

    lookupTimeMs = Duration.ofMinutes(31).toMillis();
    assertThat(table.get(1, noTtlKey, lookupTimeMs), is(val));
    assertThat(table.get(1, defaultTtlKey, lookupTimeMs), nullValue());    // expired
    assertThat(table.get(1, tenMinTtlKey, lookupTimeMs), nullValue()); // expired
    assertThat(table.get(1, fiftyMinTtlKey, lookupTimeMs), is(val));

    lookupTimeMs = Duration.ofMinutes(51).toMillis();
    assertThat(table.get(1, noTtlKey, lookupTimeMs), is(val));
    assertThat(table.get(1, defaultTtlKey, lookupTimeMs), nullValue());    // expired
    assertThat(table.get(1, tenMinTtlKey, lookupTimeMs), nullValue()); // expired
    assertThat(table.get(1, fiftyMinTtlKey, lookupTimeMs), nullValue());   // expired
  }

  @Test
  public void shouldRespectSemanticKeyValueBasedTtl() throws Exception {
    // Given:
    final var defaultTtl = Duration.ofMinutes(30);
    final BiFunction<String, String, Optional<TtlDuration>> ttlForKeyAndValue = (k, v) -> {
      if (k.equals("10_MIN_RETENTION")) {
        return Optional.of(TtlDuration.of(Duration.ofMinutes(10)));
      } else {
        if (v.equals("DEFAULT")) {
          return Optional.empty();
        } else if (v.equals("NO_TTL")) {
          return Optional.of(TtlDuration.infinite());
        } else {
          return Optional.of(TtlDuration.of(Duration.ofMinutes(Long.parseLong(v))));
        }
      }
    };
    final TtlProvider<String, String> ttlProvider = TtlProvider
        .<String, String>withDefault(defaultTtl)
        .fromKeyAndValue(ttlForKeyAndValue);
    params = ResponsiveKeyValueParams.fact(storeName).withTtlProvider(ttlProvider);


    final var table = client.factFactory().create(RemoteTableSpecFactory.fromKVParams(
        params,
        defaultPartitioner(),
        Optional.of(new TtlResolver<>(
            new StateDeserializer<>("ignored", new StringDeserializer(), new StringDeserializer()),
            ttlProvider)),
        config
    ));

    table.init(1);

    final Bytes tenMinTtlKey = serializedKey("10_MIN_RETENTION");
    final Bytes defaultTtlKey = serializedKey("DEFAULT_30_MIN_RETENTION");
    final Bytes noTtlKey = serializedKey("NO_TTL");
    final Bytes twoMinTtlKey = serializedKey("2_MIN_RETENTION");
    final Bytes fiftyMinTtlKey = serializedKey("50_MIN_RETENTION");

    final byte[] defaultTtlValue = serializedValue("DEFAULT"); // default is 30ms
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
    long lookupTimeMs = Duration.ofMinutes(3).toMillis();
    assertThat(table.get(1, tenMinTtlKey, lookupTimeMs), is(val));
    assertThat(table.get(1, defaultTtlKey, lookupTimeMs), is(defaultTtlValue));
    assertThat(table.get(1, noTtlKey, lookupTimeMs), is(noTtlValue));
    assertThat(table.get(1, twoMinTtlKey, lookupTimeMs), nullValue());            // expired
    assertThat(table.get(1, fiftyMinTtlKey, lookupTimeMs), is(fiftyMinTtlValue));

    lookupTimeMs = Duration.ofMinutes(11).toMillis();
    assertThat(table.get(1, tenMinTtlKey, lookupTimeMs), nullValue());            // expired
    assertThat(table.get(1, defaultTtlKey, lookupTimeMs), is(defaultTtlValue));
    assertThat(table.get(1, noTtlKey, lookupTimeMs), is(noTtlValue));
    assertThat(table.get(1, twoMinTtlKey, lookupTimeMs), nullValue());            // expired
    assertThat(table.get(1, fiftyMinTtlKey, lookupTimeMs), is(fiftyMinTtlValue));

    lookupTimeMs = Duration.ofMinutes(31).toMillis();
    assertThat(table.get(1, tenMinTtlKey, lookupTimeMs), nullValue());            // expired
    assertThat(table.get(1, defaultTtlKey, lookupTimeMs), nullValue());           // expired
    assertThat(table.get(1, noTtlKey, lookupTimeMs), is(noTtlValue));
    assertThat(table.get(1, twoMinTtlKey, lookupTimeMs), nullValue());            // expired
    assertThat(table.get(1, fiftyMinTtlKey, lookupTimeMs), is(fiftyMinTtlValue));

    lookupTimeMs = Duration.ofMinutes(51).toMillis();
    assertThat(table.get(1, tenMinTtlKey, lookupTimeMs), nullValue());            // expired
    assertThat(table.get(1, defaultTtlKey, lookupTimeMs), nullValue());           // expired
    assertThat(table.get(1, noTtlKey, lookupTimeMs), is(noTtlValue));
    assertThat(table.get(1, twoMinTtlKey, lookupTimeMs), nullValue());            // expired
    assertThat(table.get(1, fiftyMinTtlKey, lookupTimeMs), nullValue());          // expired
  }

  @Test
  public void shouldRespectOverridesWithValueBasedTtl() throws Exception {
    // Given:
    final var defaultTtl = Duration.ofMinutes(30);
    final Function<String, Optional<TtlDuration>> ttlForValue = v -> {
      if (v.equals("DEFAULT")) {
        return Optional.empty();
      } else if (v.equals("NO_TTL")) {
        return Optional.of(TtlDuration.infinite());
      } else {
        return Optional.of(TtlDuration.of(Duration.ofMinutes(Long.parseLong(v))));
      }
    };
    final TtlProvider<String, String> ttlProvider = TtlProvider
        .<String, String>withDefault(defaultTtl)
        .fromValue(ttlForValue);
    params = ResponsiveKeyValueParams.fact(storeName).withTtlProvider(ttlProvider);


    final var table = client.factFactory().create(RemoteTableSpecFactory.fromKVParams(
        params,
        defaultPartitioner(),
        Optional.of(new TtlResolver<>(
            new StateDeserializer<>("ignored", new StringDeserializer(), new StringDeserializer()),
            ttlProvider)
    ), config
    ));

    table.init(1);

    final Bytes key = serializedKey("ignored");

    final byte[] defaultTtlValue = serializedValue("DEFAULT"); // default is 30ms
    final byte[] noTtlValue = serializedValue("NO_TTL");
    final byte[] threeMinTtlValue = serializedValue(String.valueOf(3));
    final byte[] tenMinTtlValue = serializedValue(String.valueOf(10));

    // When
    long currentTimeMs = 0L;
    // first record set to expire at 3ms
    client.execute(table.insert(1, key, threeMinTtlValue, currentTimeMs));

    // Then:
    currentTimeMs = Duration.ofMinutes(4).toMillis();
    assertThat(table.get(1, key, currentTimeMs), nullValue()); // expired

    // insert new record with 3ms ttl -- now set to expire at 10ms
    currentTimeMs = Duration.ofMinutes(7).toMillis();
    client.execute(table.insert(1, key, threeMinTtlValue, currentTimeMs));

    // override with 10ms ttl -- now set to expire at 18ms
    currentTimeMs = Duration.ofMinutes(8).toMillis();
    client.execute(table.insert(1, key, tenMinTtlValue, currentTimeMs));

    // record should still exist after 10ms
    currentTimeMs = Duration.ofMinutes(11).toMillis();
    assertThat(table.get(1, key, currentTimeMs), is(tenMinTtlValue));

    // override with default ttl (30ms) -- now set to expire at 45ms
    currentTimeMs = Duration.ofMinutes(15).toMillis();
    client.execute(table.insert(1, key, defaultTtlValue, currentTimeMs));

    // record should still exist after 18ms
    currentTimeMs = Duration.ofMinutes(20).toMillis();
    assertThat(table.get(1, key, currentTimeMs), is(defaultTtlValue));

    // override with no ttl -- now set to never expire
    currentTimeMs = Duration.ofMinutes(30).toMillis();
    client.execute(table.insert(1, key, noTtlValue, currentTimeMs));

    // record should still exist after 45ms
    currentTimeMs = Duration.ofMinutes(50).toMillis();
    assertThat(table.get(1, key, currentTimeMs), is(noTtlValue));
  }

}