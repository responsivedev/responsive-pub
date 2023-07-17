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

import static dev.responsive.kafka.store.ResponsivePartitionedStore.PLUGIN;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
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
import dev.responsive.db.CassandraClient.MetadataRow;
import dev.responsive.db.partitioning.SubPartitioner;
import dev.responsive.utils.ResponsiveExtension;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DeleteRecordsResult;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.errors.TaskMigratedException;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
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

@ExtendWith({MockitoExtension.class, ResponsiveExtension.class})
@MockitoSettings(strictness = Strictness.LENIENT)
public class CommitBufferTest {
  private static final Bytes KEY = Bytes.wrap(ByteBuffer.allocate(4).putInt(0).array());
  private static final Bytes KEY2 = Bytes.wrap(ByteBuffer.allocate(4).putInt(1).array());
  private static final byte[] VALUE = new byte[]{1};
  private static final int KAFKA_PARTITION = 2;
  private static final FlushTriggers TRIGGERS = FlushTriggers.ALWAYS;

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
  public void shouldFlushWithCorrectEpochForPartitionsWithDataAndOffsetForBaseSubpartition() {
    // Given:
    setPartitioner(3);
    final String tableName = name;
    final CommitBuffer<Bytes> buffer = new CommitBuffer<>(
        client, tableName, changelogTp, admin, PLUGIN, true, TRIGGERS, partitioner, 1);
    buffer.init();

    final Bytes k0 = Bytes.wrap(ByteBuffer.allocate(4).putInt(0).array());
    final Bytes k1 = Bytes.wrap(ByteBuffer.allocate(4).putInt(1).array());

    // When:
    // key 0 -> subpartition 2 * 3 + 0 % 3 = 6
    // key 1 -> subpartition 2 * 3 + 1 % 3 =  7
    // nothing in subpartition 8, should not be flushed
    buffer.put(k0, VALUE);
    buffer.put(k1, VALUE);
    buffer.flush(100L);

    // Then:
    assertThat(client.get(tableName, 6, k0), is(VALUE));
    assertThat(client.get(tableName, 7, k1), is(VALUE));
    assertThat(client.metadata(tableName, 6), is(new MetadataRow(100L, 1L)));
    assertThat(client.metadata(tableName, 7), is(new MetadataRow(-1L, 1L)));
    assertThat(client.metadata(tableName, 8), is(new MetadataRow(-1L, 1L)));
  }

  @Test
  public void shouldNotFlushWithExpiredEpoch() {
    // Given:
    final String tableName = name;
    final CommitBuffer<Bytes> buffer = new CommitBuffer<>(
        client, tableName, changelogTp, admin, PLUGIN, true, TRIGGERS, partitioner, 1);
    buffer.init();
    client.execute(client.reserveEpoch(tableName, KAFKA_PARTITION, 100L));

    // When:
    final TaskMigratedException e = assertThrows(
        TaskMigratedException.class,
        () -> {
          buffer.put(KEY, VALUE);
          buffer.flush(100L);
        }
    );

    // Then:
    assertThat(
        e.getMessage(),
        Matchers.containsString("Local Epoch: 1, Persisted Epoch: 100"));
  }

  @Test
  public void shouldIncrementEpochAndReserveForAllSubpartitionsOnInit() {
    // Given:
    final String tableName = name;
    setPartitioner(2);
    final CommitBuffer<Bytes> buffer = new CommitBuffer<>(
        client, tableName, changelogTp, admin, PLUGIN, true, TRIGGERS, partitioner, 1);
    // throwaway init to initialize table
    buffer.init();
    client.execute(client.reserveEpoch(tableName, 4, 100L));
    client.execute(client.reserveEpoch(tableName, 5, 100L));

    // When:
    buffer.init();

    // Then:
    assertThat(client.metadata(tableName, 4).epoch, is(101L));
    assertThat(client.metadata(tableName, 5).epoch, is(101L));
  }

  @Test
  public void shouldTruncateTopicAfterFlushIfTruncateEnabled() {
    // Given:
    final String tableName = name;
    final CommitBuffer<Bytes> buffer = new CommitBuffer<>(
        client, tableName, changelogTp, admin, PLUGIN, true, TRIGGERS, partitioner, 1);
    buffer.init();
    buffer.put(KEY, VALUE);

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
        client, tableName, changelogTp, admin, PLUGIN, false, TRIGGERS, partitioner, 1);
    buffer.init();
    buffer.put(KEY, VALUE);

    // when:
    buffer.flush(100L);

    // Then:
    verifyNoInteractions(admin);
  }

  @Test
  public void shouldOnlyFlushWhenBufferFullWithRecordsTrigger() {
    // Given:
    final String tableName = name;
    final CommitBuffer<Bytes> buffer = new CommitBuffer<>(
        client,
        tableName,
        changelogTp,
        admin,
        PLUGIN,
        false,
        FlushTriggers.ofRecords(10),
        partitioner,
        1);
    buffer.init();

    for (byte i = 0; i < 9; i++) {
      buffer.put(Bytes.wrap(new byte[]{i}), VALUE);
    }

    // when:
    buffer.flush(9L);

    // Then:
    assertThat(client.metadata(tableName, KAFKA_PARTITION).offset, is(-1L));
    buffer.put(Bytes.wrap(new byte[]{10}), VALUE);
    buffer.flush(10L);
    assertThat(client.metadata(tableName, KAFKA_PARTITION).offset, is(10L));
  }

  @Test
  public void shouldOnlyFlushWhenBufferFullWithBytesTrigger() {
    // Given:
    final String tableName = name;
    final CommitBuffer<Bytes> buffer = new CommitBuffer<>(
        client,
        tableName,
        changelogTp,
        admin,
        PLUGIN,
        false,
        FlushTriggers.ofBytes(100),
        partitioner,
        1);
    buffer.init();
    final byte[] value = new byte[9];
    for (byte i = 0; i < 9; i++) {
      buffer.put(Bytes.wrap(new byte[]{i}), value);
    }

    // when:
    buffer.flush(9L);

    // Then:
    assertThat(client.metadata(tableName, KAFKA_PARTITION).offset, is(-1L));
    buffer.put(Bytes.wrap(new byte[]{10}), value);
    buffer.flush(10L);
    assertThat(client.metadata(tableName, KAFKA_PARTITION).offset, is(10L));
  }

  @Test
  public void shouldOnlyFlushWhenIntervalTriggerElapsed() {
    // Given:
    final String tableName = name;
    // just use an atomic reference as a mutable reference type
    final AtomicReference<Instant> clock = new AtomicReference<>(Instant.now());
    final CommitBuffer<Bytes> buffer = new CommitBuffer<>(
        client,
        tableName,
        changelogTp,
        admin,
        PLUGIN,
        false,
        FlushTriggers.ofInterval(Duration.ofSeconds(30)),
        100,
        partitioner,
        clock::get,
        1);
    buffer.init();
    buffer.put(Bytes.wrap(new byte[]{18}), VALUE);

    // When:
    buffer.flush(1L);

    // Then:
    assertThat(client.metadata(tableName, KAFKA_PARTITION).offset, is(-1L));
    clock.set(clock.get().plus(Duration.ofSeconds(35)));
    buffer.flush(5L);
    assertThat(client.metadata(tableName, KAFKA_PARTITION).offset, is(5L));
  }

  @Test
  public void shouldDeleteRowInCassandraWithTombstone() {
    // Given:
    final String table = name;
    client.execute(client.insertData(table, KAFKA_PARTITION, KEY, VALUE));
    final CommitBuffer<Bytes> buffer = new CommitBuffer<>(
        client, table, changelogTp, admin, PLUGIN, true, TRIGGERS, partitioner, 1);
    buffer.init();

    // When:
    buffer.tombstone(KEY);
    buffer.flush(100L);

    // Then:
    final byte[] value = client.get(table, KAFKA_PARTITION, KEY);
    assertThat(value, nullValue());
  }

  @Test
  public void shouldRestoreRecords() {
    // Given:
    final String tableName = name;
    final CommitBuffer<Bytes> buffer = new CommitBuffer<>(
        client, tableName, changelogTp, admin, PLUGIN, true, TRIGGERS, partitioner, 1);
    buffer.init();

    final ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(
        changelogTp.topic(), changelogTp.partition(), 100, KEY.get(), VALUE);

    // When:
    buffer.restoreBatch(List.of(record));

    // Then:
    assertThat(client.metadata(tableName, KAFKA_PARTITION).offset, is(100L));
    final byte[] value = client.get(tableName, KAFKA_PARTITION, KEY);
    assertThat(value, is(VALUE));
  }

  @Test
  public void shouldNotRestoreRecordsWhenFencedByEpoch() {
    // Given:
    final String tableName = name;
    final CommitBuffer<Bytes> buffer = new CommitBuffer<>(
        client, tableName, changelogTp, admin, PLUGIN, true, TRIGGERS, partitioner, 1);
    buffer.init();
    client.execute(client.reserveEpoch(tableName, KAFKA_PARTITION, 100)); // first epoch is 1

    final ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(
        changelogTp.topic(), KAFKA_PARTITION, 100, KEY.get(), VALUE);

    // When:
    final var e = assertThrows(
        TaskMigratedException.class,
        () -> buffer.restoreBatch(List.of(record)));

    // Then:
    assertThat(e.getMessage(), containsString("Local Epoch: 1, Persisted Epoch: 100"));
  }

  @Test
  public void shouldRestoreStreamsBatchLargerThanCassandraBatch() {
    // Given:
    final String tableName = name;
    final CommitBuffer<Bytes> buffer = new CommitBuffer<>(
        client,
        tableName,
        changelogTp,
        admin,
        PLUGIN,
        true,
        TRIGGERS,
        3,
        partitioner,
        Instant::now,
        1);
    buffer.init();
    client.execute(client.setOffset(tableName, 0, 100, 1));

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

}
