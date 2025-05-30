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

package dev.responsive.kafka.internal.db.mongo;

import static com.mongodb.MongoClientSettings.getDefaultCodecRegistry;
import static dev.responsive.kafka.api.config.ResponsiveConfig.MONGO_CONNECTION_STRING_CONFIG;
import static dev.responsive.kafka.api.config.ResponsiveConfig.MONGO_TOMBSTONE_RETENTION_SEC_CONFIG;
import static dev.responsive.kafka.internal.stores.TtlResolver.NO_TTL;
import static dev.responsive.kafka.testutils.IntegrationTestUtils.defaultOnlyTtl;
import static dev.responsive.kafka.testutils.Matchers.sameKeyValue;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import com.mongodb.client.MongoClient;
import com.mongodb.client.model.Filters;
import dev.responsive.kafka.api.config.ResponsiveConfig;
import dev.responsive.kafka.api.config.StorageBackend;
import dev.responsive.kafka.internal.stores.RemoteWriteResult;
import dev.responsive.kafka.internal.utils.SessionUtil;
import dev.responsive.kafka.testutils.IntegrationTestUtils;
import dev.responsive.kafka.testutils.ResponsiveConfigParam;
import dev.responsive.kafka.testutils.ResponsiveExtension;
import java.time.Duration;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.function.BooleanSupplier;
import org.apache.kafka.common.serialization.BytesSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.RegisterExtension;

class MongoKVTableTest {

  @RegisterExtension
  public static final ResponsiveExtension EXT = new ResponsiveExtension(StorageBackend.MONGO_DB);
  private static final CollectionCreationOptions UNSHARDED = new CollectionCreationOptions(
      false,
      0
  );

  private Map<String, Object> props;
  private String name;
  private MongoClient client;
  private ResponsiveConfig config;

  @BeforeEach
  public void before(
      final TestInfo info,
      @ResponsiveConfigParam final Map<String, Object> props
  ) {
    name = info.getDisplayName().replace("()", "");

    final String mongoConnection = (String) props.get(MONGO_CONNECTION_STRING_CONFIG);
    this.props = props;
    client = SessionUtil.connect(mongoConnection, "", null);
    config = ResponsiveConfig.responsiveConfig(props);
  }

  @Test
  public void shouldSucceedWriterWithSameEpoch() throws ExecutionException, InterruptedException {
    // Given:
    final MongoKVTable table = new MongoKVTable(client, name, UNSHARDED, NO_TTL, config);

    var writerFactory = table.init(0);
    var writer = writerFactory.createWriter(0, 0);
    writer.insert(bytes(1), byteArray(1), 100);
    writer.flush();

    // When:
    // use the same writer, which should have the same epoch
    writer.insert(bytes(1), byteArray(1), 101);
    final CompletionStage<RemoteWriteResult<Integer>> flush = writer.flush();

    // Then:
    assertThat(flush.toCompletableFuture().get().wasApplied(), is(true));
  }

  @Test
  public void shouldSucceedWriterWithLargerEpoch() throws ExecutionException, InterruptedException {
    // Given:
    var table = new MongoKVTable(client, name, UNSHARDED, NO_TTL, config);
    var writerFactory = table.init(0);
    var writer = writerFactory.createWriter(0, 0);
    writer.insert(bytes(1), byteArray(1), 100);
    writer.flush();

    // When:
    // initialize new writer with higher epoch
    table = new MongoKVTable(client, name, UNSHARDED, NO_TTL, config);
    writerFactory = table.init(0);
    writer = writerFactory.createWriter(0, 0);
    writer.insert(bytes(1), byteArray(1), 101);
    final CompletionStage<RemoteWriteResult<Integer>> flush = writer.flush();

    // Then:
    assertThat(flush.toCompletableFuture().get().wasApplied(), is(true));
  }

  @Test
  public void shouldFenceWriterSmallerEpoch() throws ExecutionException, InterruptedException {
    // Given:
    var table0 = new MongoKVTable(client, name, UNSHARDED, NO_TTL, config);
    var writerFactory0 = table0.init(0);
    var writer0 = writerFactory0.createWriter(0, 0);

    var table1 = new MongoKVTable(client, name, UNSHARDED, NO_TTL, config);
    var writerFactory1 = table1.init(0);
    var writer1 = writerFactory1.createWriter(0, 0);

    writer1.insert(bytes(1), byteArray(1), 100);
    writer1.flush();

    // When:
    // initialize new writer with higher epoch
    writer0.insert(bytes(1), byteArray(1), 101);
    final CompletionStage<RemoteWriteResult<Integer>> flush = writer0.flush();

    // Then:
    assertThat(flush.toCompletableFuture().get().wasApplied(), is(false));
  }

  @Test
  public void shouldReturnNullForNotInserted() {
    // given:
    final MongoKVTable table = new MongoKVTable(client, name, UNSHARDED, NO_TTL, config);
    table.init(0);

    // when:
    final var result = table.get(0, bytes(1), -1);

    // then:
    assertThat(result, is(nullValue()));
  }

  @Test
  public void shouldReturnNullForDeletedRecord() {
    // given:
    final MongoKVTable table = new MongoKVTable(client, name, UNSHARDED, NO_TTL, config);
    var writerFactory = table.init(0);
    var writer = writerFactory.createWriter(0, 0);
    writer.insert(bytes(1), byteArray(1), 100);
    writer.flush();
    writer = writerFactory.createWriter(0, 0);
    writer.delete(bytes(1));
    writer.flush();

    // when:
    final var result = table.get(0, bytes(1), -1);

    // then:
    assertThat(result, is(nullValue()));
  }

  @Test
  public void shouldGetInsertedRecord() {
    // given:
    final MongoKVTable table = new MongoKVTable(client, name, UNSHARDED, NO_TTL, config);
    var writerFactory = table.init(0);
    var writer = writerFactory.createWriter(0, 0);
    writer.insert(bytes(1), byteArray(1), 100);
    writer.flush();

    // when:
    final var result = table.get(0, bytes(1), -1);

    // then:
    assertThat(result.length, is(1));
    assertThat(result[0], is((byte) 1));
  }

  @Test
  public void shouldFilterResultsWithOldTimestamp() {
    // given:
    final Duration ttl = Duration.ofMillis(100);
    final MongoKVTable table = new MongoKVTable(client, name, UNSHARDED, defaultOnlyTtl(ttl),
        config);
    var writerFactory = table.init(0);
    var writer = writerFactory.createWriter(0, 0);
    writer.insert(bytes(1), byteArray(1), 0);
    writer.flush();

    // when:
    final var result = table.get(0, bytes(1), 101);

    // then:
    assertThat(result, is(nullValue()));
  }

  @Test
  public void shouldIncludeResultsWithNewerTimestamp() {
    // given:
    final Duration ttl = Duration.ofMillis(100);
    final MongoKVTable table = new MongoKVTable(client, name, UNSHARDED, defaultOnlyTtl(ttl),
        config);
    var writerFactory = table.init(0);
    var writer = writerFactory.createWriter(0, 0);
    writer.insert(bytes(1), byteArray(1), 0);
    writer.flush();

    // when:
    final var result = table.get(0, bytes(1), 100);

    // then:
    assertThat(result.length, is(1));
    assertThat(result[0], is((byte) 1));
  }

  @Test
  public void shouldHandlePartitionedRangeScansCorrectly() {
    // Given:
    final MongoKVTable table = new MongoKVTable(client, name, UNSHARDED, NO_TTL, config);

    final var writerFactory0 = table.init(0);
    final var writer0 = writerFactory0.createWriter(0, 0);
    final var writerFactory1 = table.init(1);
    final var writer1 = writerFactory1.createWriter(1, 0);

    writer0.insert(bytes(10, 11, 12, 12, 13), byteArray(1), 100);
    writer0.insert(bytes(10, 11, 12, 13), byteArray(2), 100);
    writer0.insert(bytes(10, 11, 13), byteArray(3), 100);
    writer1.insert(bytes(10, 11, 13, 13), byteArray(3), 100); // in range, excluded by partition
    writer0.insert(bytes(10, 11, 13, 14), byteArray(4), 100);
    writer0.insert(bytes(11, 12), byteArray(5), 100);

    writer0.flush();
    writer1.flush();

    // When:
    final var iter = table.range(0, bytes(10, 11, 12, 13), bytes(10, 11, 13, 14), -1);

    // Then:
    final List<KeyValue<Bytes, byte[]>> returned = new LinkedList<>();
    while (iter.hasNext()) {
      returned.add(iter.next());
    }
    assertThat(returned, contains(
        sameKeyValue(new KeyValue<>(bytes(10, 11, 12, 13), byteArray(2))),
        sameKeyValue(new KeyValue<>(bytes(10, 11, 13), byteArray(3))),
        sameKeyValue(new KeyValue<>(bytes(10, 11, 13, 14), byteArray(4)))
    ));
    iter.close();
  }

  @Test
  public void shouldHandleRangeScansCorrectlyWithNullFromKey() {
    // Given:
    final MongoKVTable table = new MongoKVTable(client, name, UNSHARDED, NO_TTL, config);

    final var writerFactory0 = table.init(0);
    final var writer0 = writerFactory0.createWriter(0, 0);

    writer0.insert(bytes(10, 11), byteArray(1), 100);
    writer0.insert(bytes(10, 11, 12), byteArray(2), 100);
    writer0.insert(bytes(11, 12, 13), byteArray(3), 100);

    writer0.flush();

    // When:
    final var iter = table.range(0, null, bytes(11, 12, 14), -1);

    // Then:
    final List<KeyValue<Bytes, byte[]>> returned = new LinkedList<>();
    while (iter.hasNext()) {
      returned.add(iter.next());
    }
    assertThat(returned, contains(
        sameKeyValue(new KeyValue<>(bytes(10, 11), byteArray(1))),
        sameKeyValue(new KeyValue<>(bytes(10, 11, 12), byteArray(2))),
        sameKeyValue(new KeyValue<>(bytes(11, 12, 13), byteArray(3)))
    ));
    iter.close();
  }

  @Test
  public void shouldHandleRangeScansCorrectlyWithNullToKey() {
    // Given:
    final MongoKVTable table = new MongoKVTable(client, name, UNSHARDED, NO_TTL, config);

    final var writerFactory0 = table.init(0);
    final var writer0 = writerFactory0.createWriter(0, 0);

    writer0.insert(bytes(10, 11), byteArray(1), 100);
    writer0.insert(bytes(10, 11, 12), byteArray(2), 100);
    writer0.insert(bytes(11, 12, 13), byteArray(3), 100);
    writer0.insert(bytes(11, 13, 14), byteArray(4), 100);

    writer0.flush();

    // When:
    final var iter = table.range(0, bytes(10, 11, 12), null, -1);

    // Then:
    final List<KeyValue<Bytes, byte[]>> returned = new LinkedList<>();
    while (iter.hasNext()) {
      returned.add(iter.next());
    }
    assertThat(returned, contains(
        sameKeyValue(new KeyValue<>(bytes(10, 11, 12), byteArray(2))),
        sameKeyValue(new KeyValue<>(bytes(11, 12, 13), byteArray(3))),
        sameKeyValue(new KeyValue<>(bytes(11, 13, 14), byteArray(4)))
    ));
    iter.close();
  }

  @Test
  public void shouldHandlePartitionedPrefixScansCorrectly() {
    // Given:
    final MongoKVTable table = new MongoKVTable(client, name, UNSHARDED, NO_TTL, config);

    final var writerFactory0 = table.init(0);
    final var writer0 = writerFactory0.createWriter(0, 0);
    final var writerFactory1 = table.init(1);
    final var writer1 = writerFactory1.createWriter(1, 0);

    writer0.insert(bytes(9, 11, 12, 12, 13), byteArray(1), 100);
    writer0.insert(bytes(10, 11, 12, 13), byteArray(2), 100);
    writer0.insert(bytes(10, 11, 13), byteArray(3), 100);
    writer1.insert(bytes(10, 11, 13, 13), byteArray(3), 100); // in prefix, excluded by partition
    writer0.insert(bytes(10, 11, 13, 14), byteArray(4), 100);
    writer0.insert(bytes(10, 12), byteArray(5), 100); // excluded by prefix

    writer0.flush();
    writer1.flush();

    // When:
    final var iter = table.prefix(bytes(10, 11), new BytesSerializer(), 0, -1);

    // Then:
    final List<KeyValue<Bytes, byte[]>> returned = new LinkedList<>();
    while (iter.hasNext()) {
      returned.add(iter.next());
    }
    assertThat(returned, contains(
        sameKeyValue(new KeyValue<>(bytes(10, 11, 12, 13), byteArray(2))),
        sameKeyValue(new KeyValue<>(bytes(10, 11, 13), byteArray(3))),
        sameKeyValue(new KeyValue<>(bytes(10, 11, 13, 14), byteArray(4)))
    ));
    iter.close();
  }

  @Test
  public void shouldHandlePartitionedPrefixScansCorrectlyWithNullUpperBound() {
    // Given:
    final MongoKVTable table = new MongoKVTable(client, name, UNSHARDED, NO_TTL, config);

    final var writerFactory0 = table.init(0);
    final var writer0 = writerFactory0.createWriter(0, 0);
    final var writerFactory1 = table.init(1);
    final var writer1 = writerFactory1.createWriter(1, 0);

    writer0.insert(bytes(-1, -1, -1), byteArray(1), 100);
    writer0.insert(bytes(-1, -1, -1, -1), byteArray(2), 100);

    writer0.flush();
    writer1.flush();

    // When:
    final var iter = table.prefix(bytes(-1, -1, -1), new BytesSerializer(), 0, -1);

    // Then:
    final List<KeyValue<Bytes, byte[]>> returned = new LinkedList<>();
    while (iter.hasNext()) {
      returned.add(iter.next());
    }
    assertThat(returned, contains(
        sameKeyValue(new KeyValue<>(bytes(-1, -1, -1), byteArray(1))),
        sameKeyValue(new KeyValue<>(bytes(-1, -1, -1, -1), byteArray(2)))
    ));
    iter.close();
  }

  @Test
  public void shouldFilterTombstonesFromRangeScans() {
    final MongoKVTable table = new MongoKVTable(client, name, UNSHARDED, NO_TTL, config);
    var writerFactory = table.init(0);
    var writer = writerFactory.createWriter(0, 0);
    writer.insert(bytes(10, 11, 12, 13), byteArray(2), 100);
    writer.insert(bytes(10, 11, 13), byteArray(3), 100);
    writer.insert(bytes(10, 11, 13, 14), byteArray(4), 100);
    writer.flush();
    writer = writerFactory.createWriter(0, 0);
    writer.delete(bytes(10, 11, 13));
    writer.flush();

    // When:
    final var iter = table.range(0, bytes(10, 11, 12, 13), bytes(10, 11, 13, 14), -1);

    // Then:
    final List<KeyValue<Bytes, byte[]>> returned = new LinkedList<>();
    while (iter.hasNext()) {
      returned.add(iter.next());
    }
    assertThat(returned, contains(
        sameKeyValue(new KeyValue<>(bytes(10, 11, 12, 13), byteArray(2))),
        sameKeyValue(new KeyValue<>(bytes(10, 11, 13, 14), byteArray(4)))
    ));
    iter.close();
  }

  @Test
  public void shouldFilterExpiredItemsFromRangeScans() {
    final Duration ttl = Duration.ofMillis(100);
    final MongoKVTable table = new MongoKVTable(client, name, UNSHARDED, defaultOnlyTtl(ttl),
        config);
    var writerFactory = table.init(0);
    var writer = writerFactory.createWriter(0, 0);
    writer.insert(bytes(10, 11, 12, 13), byteArray(2), 100);
    writer.insert(bytes(10, 11, 13), byteArray(3), 90);
    writer.insert(bytes(10, 11, 13, 14), byteArray(4), 100);
    writer.flush();

    // When:
    final var iter = table.range(0, bytes(10, 11, 12, 13), bytes(10, 11, 13, 14), 195);

    // Then:
    final List<KeyValue<Bytes, byte[]>> returned = new LinkedList<>();
    while (iter.hasNext()) {
      returned.add(iter.next());
    }
    assertThat(returned, contains(
        sameKeyValue(new KeyValue<>(bytes(10, 11, 12, 13), byteArray(2))),
        sameKeyValue(new KeyValue<>(bytes(10, 11, 13, 14), byteArray(4)))
    ));
    iter.close();
  }

  @Test
  public void shouldHandleFullScansCorrectly() {
    final MongoKVTable table = new MongoKVTable(client, name, UNSHARDED, NO_TTL, config);

    final var writerFactory0 = table.init(0);
    final var writer0 = writerFactory0.createWriter(0, 0);
    final var writerFactory1 = table.init(1);
    final var writer1 = writerFactory1.createWriter(1, 0);

    writer0.insert(bytes(10, 11, 12, 13), byteArray(2), 100);
    writer0.insert(bytes(10, 11, 13), byteArray(3), 100);
    writer0.insert(bytes(10, 11, 13, 14), byteArray(4), 100);
    writer1.insert(bytes(11, 13, 14), byteArray(5), 100); // excluded by partition

    writer0.flush();
    writer1.flush();

    // When:
    final var iter = table.all(0, -1);

    // Then:
    final List<KeyValue<Bytes, byte[]>> returned = new LinkedList<>();
    while (iter.hasNext()) {
      returned.add(iter.next());
    }
    assertThat(returned, contains(
        sameKeyValue(new KeyValue<>(bytes(10, 11, 12, 13), byteArray(2))),
        sameKeyValue(new KeyValue<>(bytes(10, 11, 13), byteArray(3))),
        sameKeyValue(new KeyValue<>(bytes(10, 11, 13, 14), byteArray(4)))
    ));
    iter.close();
  }

  @Test
  public void shouldFilterTombstonesFromFullScans() {
    final MongoKVTable table = new MongoKVTable(client, name, UNSHARDED, NO_TTL, config);
    var writerFactory = table.init(0);
    var writer = writerFactory.createWriter(0, 0);
    writer.insert(bytes(10, 11, 12, 13), byteArray(2), 100);
    writer.insert(bytes(10, 11, 13), byteArray(3), 100);
    writer.insert(bytes(10, 11, 13, 14), byteArray(4), 100);
    writer.flush();
    writer = writerFactory.createWriter(0, 0);
    writer.delete(bytes(10, 11, 13));
    writer.flush();

    // When:
    final var iter = table.all(0, -1);

    // Then:
    final List<KeyValue<Bytes, byte[]>> returned = new LinkedList<>();
    while (iter.hasNext()) {
      returned.add(iter.next());
    }
    assertThat(returned, contains(
        sameKeyValue(new KeyValue<>(bytes(10, 11, 12, 13), byteArray(2))),
        sameKeyValue(new KeyValue<>(bytes(10, 11, 13, 14), byteArray(4)))
    ));
    iter.close();
  }

  @Test
  public void shouldFilterExpiredFromFullScans() {
    final Duration ttl = Duration.ofMillis(100);
    final MongoKVTable table = new MongoKVTable(client, name, UNSHARDED, defaultOnlyTtl(ttl),
        config);

    var writerFactory = table.init(0);
    var writer = writerFactory.createWriter(0, 0);
    writer.insert(bytes(10, 11, 12, 13), byteArray(2), 100);
    writer.insert(bytes(10, 11, 13), byteArray(3), 90);
    writer.insert(bytes(10, 11, 13, 14), byteArray(4), 100);
    writer.flush();

    // When:
    final var iter = table.all(0, 195);

    // Then:
    final List<KeyValue<Bytes, byte[]>> returned = new LinkedList<>();
    while (iter.hasNext()) {
      returned.add(iter.next());
    }
    assertThat(returned, contains(
        sameKeyValue(new KeyValue<>(bytes(10, 11, 12, 13), byteArray(2))),
        sameKeyValue(new KeyValue<>(bytes(10, 11, 13, 14), byteArray(4)))
    ));
    iter.close();
  }

  @Test
  public void shouldUseDateFormatForTtlFields() {
    final var keyCodec = new StringKeyCodec();
    final CodecProvider pojoCodecProvider = PojoCodecProvider.builder().automatic(true).build();
    final CodecRegistry pojoCodecRegistry = fromRegistries(
        getDefaultCodecRegistry(),
        fromProviders(pojoCodecProvider)
    );

    final MongoKVTable table = new MongoKVTable(client, name, UNSHARDED, NO_TTL, config);
    final var writerFactory = table.init(0);
    final var writer = writerFactory.createWriter(0, 0);

    final Bytes key = bytes(1);
    final byte[] value = byteArray(1);
    final long timestampMs = 100L;

    writer.insert(key, value, timestampMs);
    writer.flush();

    final var database = client
        .getDatabase(name)
        .withCodecRegistry(pojoCodecRegistry)
        .getCollection("kv_data", KVDoc.class);

    final Date minValidTs = new Date(0L);

    final KVDoc record = database.find(Filters.and(
        Filters.eq(KVDoc.ID, keyCodec.encode(key)),
        Filters.gte(KVDoc.TIMESTAMP, minValidTs)
    )).first();

    if (record == null) {
      throw new AssertionError("Could not find record in database");
    } else {
      assertThat(record.getTimestamp(), equalTo(new Date(timestampMs)));
      assertThat(record.getTombstoneTs(), nullValue());
    }

    writer.delete(key);
    writer.flush();

    final KVDoc deletedRecord = database.find(Filters.eq(KVDoc.ID, keyCodec.encode(key))).first();

    if (deletedRecord == null) {
      throw new AssertionError("Could not find tombstone record in database");
    } else {
      assertThat(deletedRecord.getTimestamp(), nullValue());
      assertThat(deletedRecord.getTombstoneTs(), notNullValue()); // set to current time of delete
    }
  }

  /**
   * Leave this disabled since it takes about a full minute to run, and only really
   * verifies that Mongo's internal TTL mechanism works on timestamps in the Date
   * format, which is unlikely to change/break over time.
   * Note that the above test #shouldUseDateFormatForTtlFields ensures that our
   * Mongo implementation uses the Date format, and this test does run regularly.
   */
  @Disabled
  @Test
  public void shouldExpireRecords() {
    props.put(MONGO_TOMBSTONE_RETENTION_SEC_CONFIG, 1);
    final ResponsiveConfig config = ResponsiveConfig.responsiveConfig(props);
    final Duration ttl = Duration.ofMillis(100);
    final MongoKVTable table = new MongoKVTable(
        client, name, UNSHARDED, defaultOnlyTtl(ttl), config
    );

    var writerFactory = table.init(0);
    var writer = writerFactory.createWriter(0, 0);
    writer.insert(bytes(10, 11, 12, 13), byteArray(2), 100);
    writer.flush();

    // When:
    final BooleanSupplier isExpired = () -> {
      final byte[] bytes = table.get(0, bytes(10, 11, 12, 13), 100);
      return bytes == null;
    };

    // Then:
    IntegrationTestUtils.awaitCondition(isExpired, Duration.ofMinutes(2), Duration.ofSeconds(1));
  }

  private byte[] byteArray(int... bytes) {
    byte[] byteArray = new byte[bytes.length];
    for (int i = 0; i < bytes.length; i++) {
      byteArray[i] = (byte) bytes[i];
    }
    return byteArray;
  }

  private Bytes bytes(int... bytes) {
    return Bytes.wrap(byteArray(bytes));
  }
}
