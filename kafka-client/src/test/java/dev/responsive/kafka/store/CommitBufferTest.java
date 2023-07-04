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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import dev.responsive.db.CassandraClient;
import dev.responsive.utils.ContainerExtension;
import dev.responsive.utils.ExplodePartitioner;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DeleteRecordsResult;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.processor.internals.RecordCollector;
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

  private CqlSession session;
  private CassandraClient client;
  private TopicPartition changelogTp;
  private ExplodePartitioner partitioner;

  private String name;
  @Mock private RecordCollector.Supplier supplier;
  @Mock private RecordCollector collector;
  @Mock private Admin admin;

  @BeforeEach
  public void before(final TestInfo info, final CassandraContainer<?> cassandra) {
    name = info.getTestMethod().orElseThrow().getName();
    session = CqlSession.builder()
        .addContactPoint(cassandra.getContactPoint())
        .withLocalDatacenter(cassandra.getLocalDatacenter())
        .withKeyspace("responsive_clients") // NOTE: this keyspace is expected to exist
        .build();
    client = new CassandraClient(session);
    changelogTp = new TopicPartition("log", 0);
    setPartitioner(1);

    client.createDataTable(name);
    client.prepareStatements(name);

    when(supplier.recordCollector()).thenReturn(collector);
    when(collector.offsets())
        .thenReturn(Collections.singletonMap(new TopicPartition("log", 0), 100L));
    when(admin.deleteRecords(Mockito.any()))
        .thenReturn(new DeleteRecordsResult(Map.of()));
  }

  private void setPartitioner(final int factor) {
    partitioner = new ExplodePartitioner(factor);
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
        client, tableName, changelogTp, supplier, admin, ResponsiveStore.PLUGIN, partitioner);

    // When:
    for (int i = 0; i < CommitBuffer.MAX_BATCH_SIZE * 1.5; i++) {
      buffer.put(new Bytes(ByteBuffer.allocate(4).putInt(i).array()), VALUE);
    }
    buffer.flush();

    // Then:
    final byte[] key = ByteBuffer.allocate(4).putInt(0).array();
    final byte[] value = client.get(tableName, 0, Bytes.wrap(key));
    assertThat(value, is(VALUE));
    assertThat(client.getOffset(tableName, 0).offset, is(100L));
  }

  @Test
  public void shouldExplodeFlushAndUpdateOffsetWhenLargerOffset() {
    // Given:
    setPartitioner(2);
    final String tableName = name;
    final CommitBuffer<Bytes> buffer = new CommitBuffer<>(
        client, tableName, changelogTp, supplier, admin, ResponsiveStore.PLUGIN, partitioner);

    // When:
    for (int i = 0; i < CommitBuffer.MAX_BATCH_SIZE * 2.5; i++) {
      final byte[] array = ByteBuffer.allocate(4).putInt(i).array();
      buffer.put(new Bytes(array), VALUE);
    }
    buffer.flush();

    // Then:
    assertThat(client.get(tableName, 0, KEY), is(VALUE));
    assertThat(client.get(tableName, 1, KEY2), is(VALUE));

    assertThat(client.getOffset(tableName, 0).offset, is(100L));
    assertThat(client.getOffset(tableName, 1).offset, is(100L));
  }

  @Test
  public void shouldExplodeFlushWhenThereAreEmptyPartitionSets() {
    // Given:
    setPartitioner(2);
    final String tableName = name;
    final CommitBuffer<Bytes> buffer = new CommitBuffer<>(
        client, tableName, changelogTp, supplier, admin, ResponsiveStore.PLUGIN, partitioner);

    // When:
    for (int i = 0; i < CommitBuffer.MAX_BATCH_SIZE * 1.5; i++) {
      // i << 1 ensures they all go to partition 0
      final byte[] array = ByteBuffer.allocate(4).putInt(i << 1).array();
      buffer.put(new Bytes(array), VALUE);
    }
    buffer.flush();

    // Then:
    assertThat(client.get(tableName, 0, KEY), is(VALUE));

    assertThat(client.getOffset(tableName, 0).offset, is(100L));
    assertThat(client.getOffset(tableName, 1).offset, is(100L));
  }

  @Test
  public void shouldDeleteRowInCassandraWithTombstone() {
    // Given:
    final String table = name;
    client.insertData(table, 0, KEY, VALUE);
    final CommitBuffer<Bytes> buffer = new CommitBuffer<>(
        client, table, changelogTp, supplier, admin, ResponsiveStore.PLUGIN, partitioner);

    // When:
    buffer.tombstone(KEY);
    buffer.flush();

    // Then:
    final byte[] value = client.get(table, 0, KEY);
    assertThat(value, nullValue());
  }

  @Test
  public void shouldThrowErrorOnFlushWhenSmallerOffset() {
    // Given:
    final String tableName = name;
    final CommitBuffer<Bytes> buffer = new CommitBuffer<>(
        client, tableName, changelogTp, supplier, admin, ResponsiveStore.PLUGIN, partitioner);
    client.execute(client.revokePermit(tableName, 0, 101));

    // Expect
    // When:
    assertThrows(ProcessorStateException.class, () -> {
      buffer.put(KEY, VALUE);
      buffer.flush();
    }, "client was fenced");
  }

  @Test
  public void shouldThrowErrorOnFlushWhenDifferentTxnIdIsSet() {
    // Given:
    final String tableName = name;
    final CommitBuffer<Bytes> buffer = new CommitBuffer<>(
        client, tableName, changelogTp, supplier, admin, ResponsiveStore.PLUGIN, partitioner);
    client.execute(client.acquirePermit(tableName, 0, UNSET_PERMIT, UUID.randomUUID(), 1));

    // Expect
    // When:
    assertThrows(ProcessorStateException.class, () -> {
      buffer.put(KEY, VALUE);
      buffer.flush();
    }, "client was fenced");
  }

  @Test
  public void shouldThrowErrorOnFlushWhenEqualOffset() {
    // Given:
    final String tableName = name;
    final CommitBuffer<Bytes> buffer = new CommitBuffer<>(
        client, tableName, changelogTp, supplier, admin, ResponsiveStore.PLUGIN, partitioner);
    client.execute(client.revokePermit(tableName, 0, 100));

    // Expect
    // When:
    assertThrows(ProcessorStateException.class, () -> {
      buffer.put(KEY, VALUE);
      buffer.flush();
    }, "client was fenced");
  }

  @Test
  public void shouldRestoreRecordsGreaterThanCassandraOffset() {
    // Given:
    final String tableName = name;
    final CommitBuffer<Bytes> buffer = new CommitBuffer<>(
        client, tableName, changelogTp, supplier, admin, ResponsiveStore.PLUGIN, partitioner);
    client.execute(client.revokePermit(tableName, 0, 100));

    final ConsumerRecord<byte[], byte[]> ignored = new ConsumerRecord<>(
        changelogTp.topic(), changelogTp.partition(), 100, KEY.get(), new byte[]{1});
    final ConsumerRecord<byte[], byte[]> restored = new ConsumerRecord<>(
        changelogTp.topic(), changelogTp.partition(), 101, KEY2.get(), new byte[]{2});

    // When:
    buffer.restoreBatch(List.of(ignored, restored));

    // Then:
    assertThat(client.getOffset(tableName, 0).offset, is(101L));
  }

  @Test
  @SuppressWarnings({"rawtypes", "unchecked"})
  public void shouldIgnoreFlushFailuresOnRestore() {
    // Given:
    final String tableName = name;
    client.execute(client.revokePermit(tableName, 0, 100));
    final CommitBuffer<Bytes> buffer = new CommitBuffer<>(
        client, tableName, changelogTp, supplier, admin, ResponsiveStore.PLUGIN, partitioner);

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
        client.execute(client.revokePermit(tableName, 0, 102));
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

}