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

import static dev.responsive.kafka.api.config.ResponsiveConfig.MONGO_ENDPOINT_CONFIG;
import static dev.responsive.kafka.internal.stores.TtlResolver.NO_TTL;
import static dev.responsive.kafka.testutils.IntegrationTestUtils.defaultOnlyTtl;
import static dev.responsive.kafka.testutils.Matchers.sameKeyValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import com.mongodb.client.MongoClient;
import dev.responsive.kafka.api.config.StorageBackend;
import dev.responsive.kafka.internal.stores.RemoteWriteResult;
import dev.responsive.kafka.internal.utils.SessionUtil;
import dev.responsive.kafka.testutils.ResponsiveConfigParam;
import dev.responsive.kafka.testutils.ResponsiveExtension;
import java.time.Duration;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.junit.jupiter.api.BeforeEach;
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

  private String name;
  private MongoClient client;

  @BeforeEach
  public void before(
      final TestInfo info,
      @ResponsiveConfigParam final Map<String, Object> props
  ) {
    name = info.getDisplayName().replace("()", "");

    final String mongoConnection = (String) props.get(MONGO_ENDPOINT_CONFIG);
    client = SessionUtil.connect(mongoConnection, null, null, "", null);
  }

  @Test
  public void shouldSucceedWriterWithSameEpoch() throws ExecutionException, InterruptedException {
    // Given:
    final MongoKVTable table = new MongoKVTable(client, name, UNSHARDED, NO_TTL);

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
    var table = new MongoKVTable(client, name, UNSHARDED, NO_TTL);
    var writerFactory = table.init(0);
    var writer = writerFactory.createWriter(0, 0);
    writer.insert(bytes(1), byteArray(1), 100);
    writer.flush();

    // When:
    // initialize new writer with higher epoch
    table = new MongoKVTable(client, name, UNSHARDED, NO_TTL);
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
    var table0 = new MongoKVTable(client, name, UNSHARDED, NO_TTL);
    var writerFactory0 = table0.init(0);
    var writer0 = writerFactory0.createWriter(0, 0);

    var table1 = new MongoKVTable(client, name, UNSHARDED, NO_TTL);
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
    final MongoKVTable table = new MongoKVTable(client, name, UNSHARDED, NO_TTL);
    table.init(0);

    // when:
    final var result = table.get(0, bytes(1), -1);

    // then:
    assertThat(result, is(nullValue()));
  }

  @Test
  public void shouldReturnNullForDeletedRecord() {
    // given:
    final MongoKVTable table = new MongoKVTable(client, name, UNSHARDED, NO_TTL);
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
    final MongoKVTable table = new MongoKVTable(client, name, UNSHARDED, NO_TTL);
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
    final MongoKVTable table = new MongoKVTable(client, name, UNSHARDED, defaultOnlyTtl(ttl));
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
    final MongoKVTable table = new MongoKVTable(client, name, UNSHARDED, defaultOnlyTtl(ttl));
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
    final MongoKVTable table = new MongoKVTable(client, name, UNSHARDED, NO_TTL);

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
  public void shouldFilterTombstonesFromRangeScans() {
    final MongoKVTable table = new MongoKVTable(client, name, UNSHARDED, NO_TTL);
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
    final MongoKVTable table = new MongoKVTable(client, name, UNSHARDED, defaultOnlyTtl(ttl));
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
    final MongoKVTable table = new MongoKVTable(client, name, UNSHARDED, NO_TTL);

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
    final MongoKVTable table = new MongoKVTable(client, name, UNSHARDED, NO_TTL);
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
    final MongoKVTable table = new MongoKVTable(client, name, UNSHARDED, defaultOnlyTtl(ttl));

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
