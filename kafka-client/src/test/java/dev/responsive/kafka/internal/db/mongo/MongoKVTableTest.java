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

package dev.responsive.kafka.internal.db.mongo;

import static dev.responsive.kafka.api.config.ResponsiveConfig.MONGO_ENDPOINT_CONFIG;
import static dev.responsive.kafka.internal.db.testutils.Matchers.sameKeyValue;
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
    client = SessionUtil.connect(mongoConnection, null, null);
  }

  @Test
  public void shouldSucceedWriterWithSameEpoch() throws ExecutionException, InterruptedException {
    // Given:
    final MongoKVTable table = new MongoKVTable(client, name, UNSHARDED);

    var writerFactory = table.init(0);
    var writer = writerFactory.createWriter(0);
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
    var table = new MongoKVTable(client, name, UNSHARDED);
    var writerFactory = table.init(0);
    var writer = writerFactory.createWriter(0);
    writer.insert(bytes(1), byteArray(1), 100);
    writer.flush();

    // When:
    // initialize new writer with higher epoch
    table = new MongoKVTable(client, name, UNSHARDED);
    writerFactory = table.init(0);
    writer = writerFactory.createWriter(0);
    writer.insert(bytes(1), byteArray(1), 101);
    final CompletionStage<RemoteWriteResult<Integer>> flush = writer.flush();

    // Then:
    assertThat(flush.toCompletableFuture().get().wasApplied(), is(true));
  }

  @Test
  public void shouldFenceWriterSmallerEpoch() throws ExecutionException, InterruptedException {
    // Given:
    var table0 = new MongoKVTable(client, name, UNSHARDED);
    var writerFactory0 = table0.init(0);
    var writer0 = writerFactory0.createWriter(0);

    var table1 = new MongoKVTable(client, name, UNSHARDED);
    var writerFactory1 = table1.init(0);
    var writer1 = writerFactory1.createWriter(0);

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
    final MongoKVTable table = new MongoKVTable(client, name, UNSHARDED);
    table.init(0);

    // when:
    final var result = table.get(0, bytes(1), -1);

    // then:
    assertThat(result, is(nullValue()));
  }

  @Test
  public void shouldReturnNullForDeletedRecord() {
    // given:
    final MongoKVTable table = new MongoKVTable(client, name, UNSHARDED);
    var writerFactory = table.init(0);
    var writer = writerFactory.createWriter(0);
    writer.insert(bytes(1), byteArray(1), 100);
    writer.flush();
    writer = writerFactory.createWriter(0);
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
    final MongoKVTable table = new MongoKVTable(client, name, UNSHARDED);
    var writerFactory = table.init(0);
    var writer = writerFactory.createWriter(0);
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
    final MongoKVTable table = new MongoKVTable(client, name, UNSHARDED);
    var writerFactory = table.init(0);
    var writer = writerFactory.createWriter(0);
    writer.insert(bytes(1), byteArray(1), 100);
    writer.flush();

    // when:
    final var result = table.get(0, bytes(1), 101);

    // then:
    assertThat(result, is(nullValue()));
  }

  @Test
  public void shouldIncludeResultsWithNewerTimestamp() {
    // given:
    final MongoKVTable table = new MongoKVTable(client, name, UNSHARDED);
    var writerFactory = table.init(0);
    var writer = writerFactory.createWriter(0);
    writer.insert(bytes(1), byteArray(1), 100);
    writer.flush();

    // when:
    final var result = table.get(0, bytes(1), 100);

    // then:
    assertThat(result.length, is(1));
    assertThat(result[0], is((byte) 1));
  }

  @Test
  public void shouldHandleRangeScansCorrectly() {
    // Given:
    final MongoKVTable table = new MongoKVTable(client, name, UNSHARDED);
    var writerFactory = table.init(0);
    var writer = writerFactory.createWriter(0);
    writer.insert(bytes(10, 11, 12, 12, 13), byteArray(1), 100);
    writer.insert(bytes(10, 11, 12, 13), byteArray(2), 100);
    writer.insert(bytes(10, 11, 13), byteArray(3), 100);
    writer.insert(bytes(10, 11, 13, 14), byteArray(4), 100);
    writer.insert(bytes(11, 12), byteArray(5), 100);
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
        sameKeyValue(new KeyValue<>(bytes(10, 11, 13), byteArray(3))),
        sameKeyValue(new KeyValue<>(bytes(10, 11, 13, 14), byteArray(4)))
    ));
    iter.close();
  }

  @Test
  public void shouldFilterTombstonesFromRangeScans() {
    final MongoKVTable table = new MongoKVTable(client, name, UNSHARDED);
    var writerFactory = table.init(0);
    var writer = writerFactory.createWriter(0);
    writer.insert(bytes(10, 11, 12, 13), byteArray(2), 100);
    writer.insert(bytes(10, 11, 13), byteArray(3), 100);
    writer.insert(bytes(10, 11, 13, 14), byteArray(4), 100);
    writer.flush();
    writer = writerFactory.createWriter(0);
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
    final MongoKVTable table = new MongoKVTable(client, name, UNSHARDED);
    var writerFactory = table.init(0);
    var writer = writerFactory.createWriter(0);
    writer.insert(bytes(10, 11, 12, 13), byteArray(2), 100);
    writer.insert(bytes(10, 11, 13), byteArray(3), 90);
    writer.insert(bytes(10, 11, 13, 14), byteArray(4), 100);
    writer.flush();

    // When:
    final var iter = table.range(0, bytes(10, 11, 12, 13), bytes(10, 11, 13, 14), 100);

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
    final MongoKVTable table = new MongoKVTable(client, name, UNSHARDED);
    var writerFactory = table.init(0);
    var writer = writerFactory.createWriter(0);
    writer.insert(bytes(10, 11, 12, 13), byteArray(2), 100);
    writer.insert(bytes(10, 11, 13), byteArray(3), 100);
    writer.insert(bytes(10, 11, 13, 14), byteArray(4), 100);
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
        sameKeyValue(new KeyValue<>(bytes(10, 11, 13), byteArray(3))),
        sameKeyValue(new KeyValue<>(bytes(10, 11, 13, 14), byteArray(4)))
    ));
    iter.close();
  }

  @Test
  public void shouldFilterTombstonesFromFullScans() {
    final MongoKVTable table = new MongoKVTable(client, name, UNSHARDED);
    var writerFactory = table.init(0);
    var writer = writerFactory.createWriter(0);
    writer.insert(bytes(10, 11, 12, 13), byteArray(2), 100);
    writer.insert(bytes(10, 11, 13), byteArray(3), 100);
    writer.insert(bytes(10, 11, 13, 14), byteArray(4), 100);
    writer.flush();
    writer = writerFactory.createWriter(0);
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
    final MongoKVTable table = new MongoKVTable(client, name, UNSHARDED);
    var writerFactory = table.init(0);
    var writer = writerFactory.createWriter(0);
    writer.insert(bytes(10, 11, 12, 13), byteArray(2), 100);
    writer.insert(bytes(10, 11, 13), byteArray(3), 90);
    writer.insert(bytes(10, 11, 13, 14), byteArray(4), 100);
    writer.flush();

    // When:
    final var iter = table.all(0, 100);

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