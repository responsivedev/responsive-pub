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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import dev.responsive.db.CassandraClient;
import dev.responsive.utils.ContainerExtension;
import java.nio.charset.Charset;
import java.util.Collections;
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

  private static final Bytes KEY = Bytes.wrap(new byte[]{1});
  private static final byte[] VALUE = new byte[]{1};

  private CqlSession session;
  private CassandraClient client;
  private TopicPartition changelogTp;

  private String name;
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

    client.createDataTable(name);
    client.prepareStatements(name);

    when(admin.deleteRecords(Mockito.any()))
        .thenReturn(new DeleteRecordsResult(Map.of()));
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
    client.initializeOffset(tableName, 0);
    final CommitBuffer<Bytes> buffer = new CommitBuffer<>(
        client, tableName, changelogTp, admin, ResponsivePartitionedStore.PLUGIN, true, 0);

    // When:
    for (int i = 0; i < CommitBuffer.MAX_BATCH_SIZE * 1.5; i++) {
      buffer.put(KEY, VALUE);
    }
    buffer.flush(100L);

    // Then:
    final byte[] value = client.get(tableName, 0, KEY);
    assertThat(value, is(VALUE));
    assertThat(client.getOffset(tableName, 0).offset, is(100L));
  }

  @Test
  public void shouldTruncateTopicAfterFlushIfTruncateEnabled() {
    // Given:
    final String tableName = name;
    client.initializeOffset(tableName, 0);
    final CommitBuffer<Bytes> buffer = new CommitBuffer<>(
        client, tableName, changelogTp, admin, ResponsivePartitionedStore.PLUGIN, true, 0);
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
    client.initializeOffset(tableName, 0);
    final CommitBuffer<Bytes> buffer = new CommitBuffer<>(
        client, tableName, changelogTp, admin, ResponsivePartitionedStore.PLUGIN, false, 0);
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
    client.initializeOffset(tableName, 0);
    final CommitBuffer<Bytes> buffer = new CommitBuffer<>(
        client, tableName, changelogTp, admin, ResponsivePartitionedStore.PLUGIN, false, 10);
    for (byte i = 0; i < 9; i++) {
      buffer.put(Bytes.wrap(new byte[]{i}), VALUE);
    }

    // when:
    buffer.flush(9L);

    // Then:
    assertThat(client.getOffset(tableName, 0).offset, is(-1L));
    buffer.put(Bytes.wrap(new byte[]{10}), VALUE);
    buffer.flush(10L);
    assertThat(client.getOffset(tableName, 0).offset, is(10L));
  }

  @Test
  public void shouldDeleteRowInCassandraWithTombstone() {
    // Given:
    final String table = name;
    client.initializeOffset(table, 0);
    client.insertData(table, 0, KEY, VALUE);
    final CommitBuffer<Bytes> buffer = new CommitBuffer<>(
        client, table, changelogTp, admin, ResponsivePartitionedStore.PLUGIN, true, 0);

    // When:
    buffer.tombstone(Bytes.wrap(new byte[]{1}));
    buffer.flush(100L);

    // Then:
    final byte[] value = client.get(table, 0, KEY);
    assertThat(value, nullValue());
  }

  @Test
  public void shouldThrowErrorOnFlushWhenSmallerOffset() {
    // Given:
    final String tableName = name;
    client.initializeOffset(tableName, 0);
    client.execute(client.revokePermit(tableName, 0, 101));
    final CommitBuffer<Bytes> buffer = new CommitBuffer<>(
        client, tableName, changelogTp, admin, ResponsivePartitionedStore.PLUGIN, true, 0);

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
    client.initializeOffset(tableName, 0);
    client.execute(client.acquirePermit(tableName, 0, UNSET_PERMIT, UUID.randomUUID(), 1));
    final CommitBuffer<Bytes> buffer = new CommitBuffer<>(
        client, tableName, changelogTp, admin, ResponsivePartitionedStore.PLUGIN, true, 0);

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
    client.initializeOffset(tableName, 0);
    client.execute(client.revokePermit(tableName, 0, 100));
    final CommitBuffer<Bytes> buffer = new CommitBuffer<>(
        client, tableName, changelogTp, admin, ResponsivePartitionedStore.PLUGIN, true, 0);

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
    client.initializeOffset(tableName, 0);
    client.execute(client.revokePermit(tableName, 0, 100));
    final CommitBuffer<Bytes> buffer = new CommitBuffer<>(
        client, tableName, changelogTp, admin, ResponsivePartitionedStore.PLUGIN, true, 0);

    final ConsumerRecord<byte[], byte[]> ignored = new ConsumerRecord<>(
        changelogTp.topic(), changelogTp.partition(), 100, new byte[]{1}, new byte[]{1});
    final ConsumerRecord<byte[], byte[]> restored = new ConsumerRecord<>(
        changelogTp.topic(), changelogTp.partition(), 101, new byte[]{2}, new byte[]{2});

    // When:
    buffer.restoreBatch(List.of(ignored, restored));

    // Then:
    assertThat(client.getOffset(tableName, 0).offset, is(101L));
  }

  @Test
  public void shouldRestoreStreamsBatchLargerThanCassandraBatch() {
    // Given:
    final String tableName = name;
    client.initializeOffset(tableName, 0);
    client.execute(client.revokePermit(tableName, 0, 100));
    final CommitBuffer<Bytes> buffer = new CommitBuffer<>(
        client, tableName, changelogTp, admin, ResponsivePartitionedStore.PLUGIN, true, 0, 3);
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
              0,
              Bytes.wrap(Integer.toString(i).getBytes(Charset.defaultCharset()))),
          is(Integer.toString(i).getBytes(Charset.defaultCharset())));
    }
  }

  @Test
  @SuppressWarnings({"rawtypes", "unchecked"})
  public void shouldIgnoreFlushFailuresOnRestore() {
    // Given:
    final String tableName = name;
    client.initializeOffset(tableName, 0);
    client.execute(client.revokePermit(tableName, 0, 100));
    final CommitBuffer<Bytes> buffer = new CommitBuffer<>(
        client, tableName, changelogTp, admin, ResponsivePartitionedStore.PLUGIN, true, 0);

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
