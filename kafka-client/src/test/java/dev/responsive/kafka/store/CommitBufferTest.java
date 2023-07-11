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

package dev.responsive.kafka.store;

import static dev.responsive.db.CassandraClient.UNSET_PERMIT;
import static dev.responsive.kafka.store.ResponsivePartitionedStore.PLUGIN;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatementBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import dev.responsive.db.CassandraClient;
import dev.responsive.utils.ContainerExtension;
import dev.responsive.utils.SubPartitioner;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DeleteRecordsResult;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.testcontainers.containers.CassandraContainer;

@ExtendWith({MockitoExtension.class, ContainerExtension.class})
@MockitoSettings(strictness = Strictness.LENIENT)
public class CommitBufferTest {

  private static final Bytes KEY = Bytes.wrap(ByteBuffer.allocate(4).putInt(0).array());
  private static final Bytes KEY2 = Bytes.wrap(ByteBuffer.allocate(4).putInt(1).array());
  private static final byte[] VALUE = new byte[]{1};
  private static final int KAFKA_PARTITION = 2;

  private CqlSession session;
  private CassandraClient client;
  private TopicPartition changelogTp;

  private String name;
  @Mock private Admin admin;
  private SubPartitioner partitioner;

  @BeforeEach
  public void before(final TestInfo info, final CassandraContainer<?> cassandra) {
    name = info.getTestMethod().orElseThrow().getName();
    session = CqlSession.builder()
        .addContactPoint(cassandra.getContactPoint())
        .withLocalDatacenter(cassandra.getLocalDatacenter())
        .withKeyspace("responsive_clients") // NOTE: this keyspace is expected to exist
        .build();
    client = new CassandraClient(session);
    changelogTp = new TopicPartition("log", KAFKA_PARTITION);
    partitioner = SubPartitioner.NO_SUBPARTITIONS;

    client.createDataTable(name);
    client.prepareStatements(name);

    when(admin.deleteRecords(Mockito.any()))
        .thenReturn(new DeleteRecordsResult(Map.of()));
  }

  private void setPartitioner(final int factor) {
    partitioner = new SubPartitioner(factor, k -> ByteBuffer.wrap(k.get()).getInt());
  }

  @AfterEach
  public void after() {
    session.execute(SchemaBuilder.dropTable(name).build());
    session.close();
  }

  @Test
  public void shouldFlushAndUpdateOffsetWhenLargerOffset() {
    // Given:
    final String tableName = name;
    final CommitBuffer<Bytes> buffer = new CommitBuffer<>(
        client, tableName, changelogTp, admin, PLUGIN, true, 0, partitioner);
    buffer.init();

    // When:
    for (int i = 0; i < CommitBuffer.MAX_BATCH_SIZE * 1.5; i++) {
      buffer.put(KEY, VALUE);
    }
    buffer.flush(100L);

    // Then:
    final byte[] value = client.get(tableName, KAFKA_PARTITION, KEY);
    assertThat(value, is(VALUE));
    assertThat(client.getOffset(tableName, KAFKA_PARTITION).offset, is(100L));
  }

  @Test
  public void shouldExplodeFlushAndUpdateOffsetWhenLargerOffset() {
    // Given:
    setPartitioner(2);
    final String tableName = name;
    final CommitBuffer<Bytes> buffer = new CommitBuffer<>(
        client, tableName, changelogTp, admin, PLUGIN, true, 0, partitioner);
    buffer.init();

    // When:
    for (int i = 0; i < CommitBuffer.MAX_BATCH_SIZE * 2.5; i++) {
      final byte[] array = ByteBuffer.allocate(4).putInt(i).array();
      buffer.put(new Bytes(array), VALUE);
    }
    buffer.flush(100L);

    // Then:
    assertThat(client.get(tableName, KAFKA_PARTITION * 2, KEY), is(VALUE));
    assertThat(client.get(tableName, KAFKA_PARTITION * 2 + 1, KEY2), is(VALUE));

    assertThat(client.getOffset(tableName, KAFKA_PARTITION * 2).offset, is(100L));
    assertThat(client.getOffset(tableName, KAFKA_PARTITION * 2 + 1).offset, is(100L));
  }

  @Test
  public void shouldExplodeFlushWhenThereAreEmptyPartitionSets() {
    // Given:
    setPartitioner(2);
    final String tableName = name;
    final CommitBuffer<Bytes> buffer = new CommitBuffer<>(
        client, tableName, changelogTp, admin, PLUGIN, true, 0, partitioner);
    buffer.init();

    // When:
    for (int i = 0; i < CommitBuffer.MAX_BATCH_SIZE * 1.5; i++) {
      // i << 1 ensures they all go to partition 0
      final byte[] array = ByteBuffer.allocate(4).putInt(i << 1).array();
      buffer.put(new Bytes(array), VALUE);
    }
    buffer.flush(100L);

    // Then:
    assertThat(client.get(tableName, KAFKA_PARTITION * 2, KEY), is(VALUE));

    assertThat(client.getOffset(tableName, KAFKA_PARTITION * 2).offset, is(100L));
    assertThat(client.getOffset(tableName, KAFKA_PARTITION * 2 + 1).offset, is(100L));
  }

  @Test
  public void shouldTruncateTopicAfterFlushIfTruncateEnabled() {
    // Given:
    final String tableName = name;
    final CommitBuffer<Bytes> buffer = new CommitBuffer<>(
        client, tableName, changelogTp, admin, PLUGIN, true, 0, partitioner);
    buffer.init();
    for (int i = 0; i < CommitBuffer.MAX_BATCH_SIZE * 1.5; i++) {
      buffer.put(KEY, VALUE);
    }

    // when:
    buffer.flush(100L);

    // Then:
    verify(admin).deleteRecords(any());
  }

  @Test
  public void shouldNotTruncateTopicAfterFlushIfTruncateDisabled() {
    // Given:
    final String tableName = name;
    final CommitBuffer<Bytes> buffer = new CommitBuffer<>(
        client, tableName, changelogTp, admin, PLUGIN, false, 0, partitioner);
    buffer.init();

    for (int i = 0; i < CommitBuffer.MAX_BATCH_SIZE * 1.5; i++) {
      buffer.put(KEY, VALUE);
    }

    // when:
    buffer.flush(100L);

    // Then:
    verifyNoInteractions(admin);
  }

  @Test
  public void shouldOnlyFlushWhenBufferFull() {
    // Given:
    final String tableName = name;
    final CommitBuffer<Bytes> buffer = new CommitBuffer<>(
        client, tableName, changelogTp, admin, PLUGIN, false, 10, partitioner);
    buffer.init();

    for (byte i = 0; i < 9; i++) {
      buffer.put(Bytes.wrap(new byte[]{i}), VALUE);
    }

    // when:
    buffer.flush(9L);

    // Then:
    assertThat(client.getOffset(tableName, KAFKA_PARTITION).offset, is(-1L));
    buffer.put(Bytes.wrap(new byte[]{10}), VALUE);
    buffer.flush(10L);
    assertThat(client.getOffset(tableName, KAFKA_PARTITION).offset, is(10L));
  }

  @Test
  public void shouldDeleteRowInCassandraWithTombstone() {
    // Given:
    final String table = name;
    client.execute(client.insertData(table, KAFKA_PARTITION, KEY, VALUE));
    final CommitBuffer<Bytes> buffer = new CommitBuffer<>(
        client, table, changelogTp, admin, PLUGIN, true, 0, partitioner);
    buffer.init();

    // When:
    buffer.tombstone(KEY);
    buffer.flush(100L);

    // Then:
    final byte[] value = client.get(table, KAFKA_PARTITION, KEY);
    assertThat(value, nullValue());
  }

  @Test
  public void shouldThrowErrorOnFlushWhenSmallerOffset() {
    // Given:
    final String tableName = name;
    final CommitBuffer<Bytes> buffer = new CommitBuffer<>(
        client, tableName, changelogTp, admin, PLUGIN, true, 0, partitioner);
    buffer.init();
    client.execute(client.revokePermit(tableName, KAFKA_PARTITION, 101));

    // Expect
    // When:
    assertThrows(ProcessorStateException.class, () -> {
      buffer.put(KEY, VALUE);
      buffer.flush(100L);
    }, "client was fenced");
  }

  @Test
  public void shouldThrowErrorOnFlushWhenDifferentTxnIdIsSet() {
    // Given:
    final String tableName = name;
    final CommitBuffer<Bytes> buffer = new CommitBuffer<>(
        client, tableName, changelogTp, admin, PLUGIN, true, 0, partitioner);
    buffer.init();
    client.execute(
        client.acquirePermit(tableName, KAFKA_PARTITION, UNSET_PERMIT, UUID.randomUUID(), 1));

    // Expect
    // When:
    assertThrows(ProcessorStateException.class, () -> {
      buffer.put(KEY, VALUE);
      buffer.flush(100L);
    }, "client was fenced");
  }

  @Test
  public void shouldThrowErrorOnFlushWhenEqualOffset() {
    // Given:
    final String tableName = name;
    final CommitBuffer<Bytes> buffer = new CommitBuffer<>(
        client, tableName, changelogTp, admin, PLUGIN, true, 0, partitioner);
    buffer.init();
    client.execute(client.revokePermit(tableName, KAFKA_PARTITION, 100));

    // Expect
    // When:
    assertThrows(ProcessorStateException.class, () -> {
      buffer.put(KEY, VALUE);
      buffer.flush(100L);
    }, "client was fenced");
  }

  @Test
  public void shouldRestoreRecordsGreaterThanCassandraOffset() {
    // Given:
    final String tableName = name;
    final CommitBuffer<Bytes> buffer = new CommitBuffer<>(
        client, tableName, changelogTp, admin, PLUGIN, true, 0, partitioner);
    buffer.init();
    client.execute(client.revokePermit(tableName, KAFKA_PARTITION, 100));

    final ConsumerRecord<byte[], byte[]> ignored = new ConsumerRecord<>(
        changelogTp.topic(), changelogTp.partition(), 100, new byte[]{1}, new byte[]{1});
    final ConsumerRecord<byte[], byte[]> restored = new ConsumerRecord<>(
        changelogTp.topic(), changelogTp.partition(), 101, new byte[]{2}, new byte[]{2});

    // When:
    buffer.restoreBatch(List.of(ignored, restored));

    // Then:
    assertThat(client.getOffset(tableName, KAFKA_PARTITION).offset, is(101L));
  }

  @Test
  public void shouldRestoreStreamsBatchLargerThanCassandraBatch() {
    // Given:
    final String tableName = name;
    final CommitBuffer<Bytes> buffer = new CommitBuffer<>(
        client, tableName, changelogTp, admin, PLUGIN, true, 0, 3, partitioner);
    buffer.init();
    client.execute(client.revokePermit(tableName, 0, 100));

    final List<ConsumerRecord<byte[], byte[]>> records = IntStream.range(0, 5)
        .mapToObj(i -> new ConsumerRecord<>(
            changelogTp.topic(),
            changelogTp.partition(),
            101 + i,
            Integer.toString(i).getBytes(Charset.defaultCharset()),
            Integer.toString(i).getBytes(Charset.defaultCharset())))
        .collect(Collectors.toList());

    // when:
    buffer.restoreBatch(records);

    // then:
    for (int i = 0; i < 5; i++) {
      assertThat(
          client.get(
              tableName,
              KAFKA_PARTITION,
              Bytes.wrap(Integer.toString(i).getBytes(Charset.defaultCharset()))),
          is(Integer.toString(i).getBytes(Charset.defaultCharset())));
    }
  }

  @Test
  @SuppressWarnings({"rawtypes", "unchecked"})
  public void shouldIgnoreFlushFailuresOnRestore() {
    // Given:
    final String tableName = name;
    final CommitBuffer<Bytes> buffer = new CommitBuffer<>(
        client, tableName, changelogTp, admin, PLUGIN, true, 0, partitioner);
    buffer.init();
    client.execute(client.revokePermit(tableName, KAFKA_PARTITION, 100));

    final CountDownLatch latch1 = new CountDownLatch(0);
    final CountDownLatch latch2 = new CountDownLatch(0);

    final ConsumerRecord restoreRecord = Mockito.mock(ConsumerRecord.class);
    when(restoreRecord.key()).thenAnswer(iom -> {
      latch2.countDown();
      latch1.await();
      return KEY.get();
    });
    when(restoreRecord.value()).thenReturn(VALUE);
    when(restoreRecord.offset()).thenReturn(101L);

    // When:
    final ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      executor.submit(() -> {
        latch2.await();
        client.execute(client.revokePermit(tableName, KAFKA_PARTITION, 102));
        latch1.countDown();
        return null;
      });
      buffer.restoreBatch(List.of(restoreRecord));
    } finally {
      executor.shutdown();
    }

    // Then:
    assertThat(buffer.size(), is(0)); // nothing should have been added
  }

  @Test
  public void shouldRevokeCorruptedPermitsOnInit() {
    // Given:
    final String tableName = name;
    final CommitBuffer<Bytes> buffer = new CommitBuffer<>(
        client, tableName, changelogTp, admin, PLUGIN, true, 0, partitioner);
    // first initialize the offsets
    client.initializeOffset(tableName, 0);

    // simulate that offset 100 was already written
    client.execute(client.revokePermit(tableName, 0, 100L));
    client.execute(client.acquirePermit(tableName, 0, UNSET_PERMIT, UUID.randomUUID(), 100L));

    // When:
    buffer.init();

    // Then:
    final var result = client.execute(
        new SimpleStatementBuilder(
            "SELECT offset, permit FROM " + tableName + " WHERE partitionKey = 0"
        ).build()
    ).all();

    assertThat(result.size(), is(1));
    assertThat(result.get(0).get("offset", Long.class), is(100L));
    assertThat(result.get(0).get("permit", UUID.class), is(UNSET_PERMIT));
  }

}
