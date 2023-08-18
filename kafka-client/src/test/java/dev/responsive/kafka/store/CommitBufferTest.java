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
import dev.responsive.db.BytesKeySpec;
import dev.responsive.db.CassandraClient;
import dev.responsive.db.CassandraKeyValueSchema;
import dev.responsive.db.KeySpec;
import dev.responsive.db.LwtWriterFactory;
import dev.responsive.db.MetadataRow;
import dev.responsive.db.RemoteKeyValueSchema;
import dev.responsive.db.partitioning.SubPartitioner;
import dev.responsive.kafka.config.ResponsiveConfig;
import dev.responsive.utils.ResponsiveConfigParam;
import dev.responsive.utils.ResponsiveExtension;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
  private static final KeySpec<Bytes> KEY_SPEC = new BytesKeySpec();
  private static final Bytes KEY = Bytes.wrap(ByteBuffer.allocate(4).putInt(0).array());
  private static final byte[] VALUE = new byte[]{1};
  private static final int KAFKA_PARTITION = 2;
  private static final FlushTriggers TRIGGERS = FlushTriggers.ALWAYS;
  private static final long CURRENT_TS = 100L;
  private static final long MIN_VALID_TS = 0L;

  private CqlSession session;
  private CassandraClient client;
  private TopicPartition changelogTp;

  private String name;
  @Mock private Admin admin;
  private SubPartitioner partitioner;
  private RemoteKeyValueSchema schema;

  @BeforeEach
  public void before(
      final TestInfo info,
      final CassandraContainer<?> cassandra,
      @ResponsiveConfigParam final ResponsiveConfig config
  ) {
    name = info.getTestMethod().orElseThrow().getName();
    session = CqlSession.builder()
        .addContactPoint(cassandra.getContactPoint())
        .withLocalDatacenter(cassandra.getLocalDatacenter())
        .withKeyspace("responsive_clients") // NOTE: this keyspace is expected to exist
        .build();
    client = new CassandraClient(session, config);
    schema = new CassandraKeyValueSchema(client);
    changelogTp = new TopicPartition("log", KAFKA_PARTITION);
    partitioner = SubPartitioner.NO_SUBPARTITIONS;

    schema.create(name, Optional.empty());
    schema.prepare(name);

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
    final CommitBuffer<Bytes, RemoteKeyValueSchema> buffer = new CommitBuffer<>(
        client, tableName, changelogTp, admin, schema,
        KEY_SPEC, true, TRIGGERS, partitioner);
    buffer.init();

    // reserve epoch for partition 8 to ensure it doesn't get flushed
    // if it did it would get fenced
    LwtWriterFactory.reserve(schema, tableName, new int[]{8}, KAFKA_PARTITION, 3L, false);

    final Bytes k0 = Bytes.wrap(ByteBuffer.allocate(4).putInt(0).array());
    final Bytes k1 = Bytes.wrap(ByteBuffer.allocate(4).putInt(1).array());

    // When:
    // key 0 -> subpartition 2 * 3 + 0 % 3 = 6
    // key 1 -> subpartition 2 * 3 + 1 % 3 =  7
    // nothing in subpartition 8, should not be flushed - if it did
    // then an error would be thrown with invalid epoch
    buffer.put(k0, VALUE, CURRENT_TS);
    buffer.put(k1, VALUE, CURRENT_TS);
    buffer.flush(100L);

    // Then:
    assertThat(schema.get(tableName, 6, k0, MIN_VALID_TS), is(VALUE));
    assertThat(schema.get(tableName, 7, k1, MIN_VALID_TS), is(VALUE));
    assertThat(schema.metadata(tableName, 6), is(new MetadataRow(100L, 1L)));
    assertThat(schema.metadata(tableName, 7), is(new MetadataRow(-1L, 1L)));
    assertThat(schema.metadata(tableName, 8), is(new MetadataRow(-1L, 3L)));
  }

  @Test
  public void shouldNotFlushWithExpiredEpoch() {
    // Given:
    final String tableName = name;
    final CommitBuffer<Bytes, RemoteKeyValueSchema> buffer = new CommitBuffer<>(
        client, tableName, changelogTp, admin, schema,
        KEY_SPEC, true, TRIGGERS, partitioner);
    buffer.init();

    LwtWriterFactory.reserve(
        schema, tableName, new int[]{KAFKA_PARTITION}, KAFKA_PARTITION, 100L, false);

    // When:
    final TaskMigratedException e = assertThrows(
        TaskMigratedException.class,
        () -> {
          buffer.put(KEY, VALUE, CURRENT_TS);
          buffer.flush(100L);
        }
    );

    // Then:
    assertThat(
        e.getMessage(),
        Matchers.containsString("Local Epoch: LwtWriterFactory{epoch=1}, Persisted Epoch: 100"));
  }

  @Test
  public void shouldIncrementEpochAndReserveForAllSubpartitionsOnInit() {
    // Given:
    final String tableName = name;
    setPartitioner(2);
    final CommitBuffer<Bytes, RemoteKeyValueSchema> buffer = new CommitBuffer<>(
        client, tableName, changelogTp, admin, schema,
        KEY_SPEC, true, TRIGGERS, partitioner);
    // throwaway init to initialize table
    buffer.init();

    LwtWriterFactory.reserve(schema, tableName, new int[]{4}, KAFKA_PARTITION, 100L, false);
    LwtWriterFactory.reserve(schema, tableName, new int[]{5}, KAFKA_PARTITION, 100L, false);

    // When:
    buffer.init();

    // Then:
    assertThat(schema.metadata(tableName, 4).epoch, is(101L));
    assertThat(schema.metadata(tableName, 5).epoch, is(101L));
  }

  @Test
  public void shouldTruncateTopicAfterFlushIfTruncateEnabled() {
    // Given:
    final String tableName = name;
    final CommitBuffer<Bytes, RemoteKeyValueSchema> buffer = new CommitBuffer<>(
        client, tableName, changelogTp, admin, schema,
        KEY_SPEC, true, TRIGGERS, partitioner);
    buffer.init();
    buffer.put(KEY, VALUE, CURRENT_TS);

    // when:
    buffer.flush(100L);

    // Then:
    verify(admin).deleteRecords(any());
  }

  @Test
  public void shouldNotTruncateTopicAfterFlushIfTruncateDisabled() {
    // Given:
    final String tableName = name;
    final CommitBuffer<Bytes, RemoteKeyValueSchema> buffer = new CommitBuffer<>(
        client, tableName, changelogTp, admin, schema,
        KEY_SPEC, false, TRIGGERS, partitioner);
    buffer.init();
    buffer.put(KEY, VALUE, CURRENT_TS);

    // when:
    buffer.flush(100L);

    // Then:
    verifyNoInteractions(admin);
  }

  @Test
  public void shouldOnlyFlushWhenBufferFullWithRecordsTrigger() {
    // Given:
    final String tableName = name;
    final CommitBuffer<Bytes, RemoteKeyValueSchema> buffer = new CommitBuffer<>(
        client,
        tableName,
        changelogTp,
        admin,
        schema,
        KEY_SPEC,
        false,
        FlushTriggers.ofRecords(10),
        partitioner
    );
    buffer.init();

    for (byte i = 0; i < 9; i++) {
      buffer.put(Bytes.wrap(new byte[]{i}), VALUE, CURRENT_TS);
    }

    // when:
    buffer.flush(9L);

    // Then:
    assertThat(schema.metadata(tableName, KAFKA_PARTITION).offset, is(-1L));
    buffer.put(Bytes.wrap(new byte[]{10}), VALUE, CURRENT_TS);
    buffer.flush(10L);
    assertThat(schema.metadata(tableName, KAFKA_PARTITION).offset, is(10L));
  }

  @Test
  public void shouldOnlyFlushWhenBufferFullWithBytesTrigger() {
    // Given:
    final String tableName = name;
    final CommitBuffer<Bytes, RemoteKeyValueSchema> buffer = new CommitBuffer<>(
        client,
        tableName,
        changelogTp,
        admin,
        schema,
        KEY_SPEC,
        false,
        FlushTriggers.ofBytes(100),
        partitioner
    );
    buffer.init();
    final byte[] value = new byte[9];
    for (byte i = 0; i < 9; i++) {
      buffer.put(Bytes.wrap(new byte[]{i}), value, CURRENT_TS);
    }

    // when:
    buffer.flush(9L);

    // Then:
    assertThat(schema.metadata(tableName, KAFKA_PARTITION).offset, is(-1L));
    buffer.put(Bytes.wrap(new byte[]{10}), value, CURRENT_TS);
    buffer.flush(10L);
    assertThat(schema.metadata(tableName, KAFKA_PARTITION).offset, is(10L));
  }

  @Test
  public void shouldOnlyFlushWhenIntervalTriggerElapsed() {
    // Given:
    final String tableName = name;
    // just use an atomic reference as a mutable reference type
    final AtomicReference<Instant> clock = new AtomicReference<>(Instant.now());
    final CommitBuffer<Bytes, RemoteKeyValueSchema> buffer = new CommitBuffer<>(
        client,
        tableName,
        changelogTp,
        admin,
        schema,
        KEY_SPEC,
        false,
        FlushTriggers.ofInterval(Duration.ofSeconds(30)),
        100,
        partitioner,
        clock::get
    );
    buffer.init();
    buffer.put(Bytes.wrap(new byte[]{18}), VALUE, CURRENT_TS);

    // When:
    buffer.flush(1L);

    // Then:
    assertThat(schema.metadata(tableName, KAFKA_PARTITION).offset, is(-1L));
    clock.set(clock.get().plus(Duration.ofSeconds(35)));
    buffer.flush(5L);
    assertThat(schema.metadata(tableName, KAFKA_PARTITION).offset, is(5L));
  }

  @Test
  public void shouldDeleteRowInCassandraWithTombstone() {
    // Given:
    final String table = name;
    client.execute(schema.insert(table, KAFKA_PARTITION, KEY, VALUE, CURRENT_TS));
    final CommitBuffer<Bytes, RemoteKeyValueSchema> buffer = new CommitBuffer<>(
        client, table, changelogTp, admin, schema, KEY_SPEC, true, TRIGGERS, partitioner);
    buffer.init();

    // When:
    buffer.tombstone(KEY, CURRENT_TS);
    buffer.flush(100L);

    // Then:
    final byte[] value = schema.get(table, KAFKA_PARTITION, KEY, CURRENT_TS);
    assertThat(value, nullValue());
  }

  @Test
  public void shouldRestoreRecords() {
    // Given:
    final String tableName = name;
    final CommitBuffer<Bytes, RemoteKeyValueSchema> buffer = new CommitBuffer<>(
        client, tableName, changelogTp, admin, schema,
        KEY_SPEC, true, TRIGGERS, partitioner);
    buffer.init();

    final ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(
        changelogTp.topic(), changelogTp.partition(), 100, KEY.get(), VALUE);

    // When:
    buffer.restoreBatch(List.of(record));

    // Then:
    assertThat(schema.metadata(tableName, KAFKA_PARTITION).offset, is(100L));
    final byte[] value = schema.get(tableName, KAFKA_PARTITION, KEY, MIN_VALID_TS);
    assertThat(value, is(VALUE));
  }

  @Test
  public void shouldNotRestoreRecordsWhenFencedByEpoch() {
    // Given:
    final String tableName = name;
    final CommitBuffer<Bytes, RemoteKeyValueSchema> buffer = new CommitBuffer<>(
        client, tableName, changelogTp, admin, schema,
        KEY_SPEC, true, TRIGGERS, partitioner);
    buffer.init();
    LwtWriterFactory.reserve(
        schema, tableName, new int[]{KAFKA_PARTITION}, KAFKA_PARTITION, 100L, false);

    final ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(
        changelogTp.topic(), KAFKA_PARTITION, 100, KEY.get(), VALUE);

    // When:
    final var e = assertThrows(
        TaskMigratedException.class,
        () -> buffer.restoreBatch(List.of(record)));

    // Then:
    assertThat(e.getMessage(), containsString("Local Epoch: LwtWriterFactory{epoch=1}, "
        + "Persisted Epoch: 100"));
  }

  @Test
  public void shouldRestoreStreamsBatchLargerThanCassandraBatch() {
    // Given:
    final String tableName = name;
    final CommitBuffer<Bytes, RemoteKeyValueSchema> buffer = new CommitBuffer<>(
        client,
        tableName,
        changelogTp,
        admin,
        schema,
        KEY_SPEC,
        true,
        TRIGGERS,
        3,
        partitioner,
        Instant::now
    );
    buffer.init();
    client.execute(schema.setOffset(tableName, 100, 1));

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
          schema.get(
              tableName,
              KAFKA_PARTITION,
              Bytes.wrap(Integer.toString(i).getBytes(Charset.defaultCharset())),
              MIN_VALID_TS),
          is(Integer.toString(i).getBytes(Charset.defaultCharset())));
    }
  }

}
