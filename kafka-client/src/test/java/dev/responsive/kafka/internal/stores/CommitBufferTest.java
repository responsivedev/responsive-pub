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

package dev.responsive.kafka.internal.stores;

/*
import static dev.responsive.kafka.internal.metrics.StoreMetrics.FAILED_TRUNCATIONS_RATE;
import static dev.responsive.kafka.internal.metrics.StoreMetrics.FAILED_TRUNCATIONS_RATE_DESCRIPTION;
import static dev.responsive.kafka.internal.metrics.StoreMetrics.FAILED_TRUNCATIONS_TOTAL;
import static dev.responsive.kafka.internal.metrics.StoreMetrics.FAILED_TRUNCATIONS_TOTAL_DESCRIPTION;
import static dev.responsive.kafka.internal.metrics.StoreMetrics.FLUSH_ERRORS_RATE;
import static dev.responsive.kafka.internal.metrics.StoreMetrics.FLUSH_ERRORS_RATE_DESCRIPTION;
import static dev.responsive.kafka.internal.metrics.StoreMetrics.FLUSH_ERRORS_TOTAL;
import static dev.responsive.kafka.internal.metrics.StoreMetrics.FLUSH_ERRORS_TOTAL_DESCRIPTION;
import static dev.responsive.kafka.internal.metrics.StoreMetrics.FLUSH_LATENCY_AVG;
import static dev.responsive.kafka.internal.metrics.StoreMetrics.FLUSH_LATENCY_AVG_DESCRIPTION;
import static dev.responsive.kafka.internal.metrics.StoreMetrics.FLUSH_LATENCY_MAX;
import static dev.responsive.kafka.internal.metrics.StoreMetrics.FLUSH_LATENCY_MAX_DESCRIPTION;
import static dev.responsive.kafka.internal.metrics.StoreMetrics.FLUSH_RATE;
import static dev.responsive.kafka.internal.metrics.StoreMetrics.FLUSH_RATE_DESCRIPTION;
import static dev.responsive.kafka.internal.metrics.StoreMetrics.FLUSH_TOTAL;
import static dev.responsive.kafka.internal.metrics.StoreMetrics.FLUSH_TOTAL_DESCRIPTION;
import static dev.responsive.kafka.internal.metrics.StoreMetrics.TIME_SINCE_LAST_FLUSH;
import static dev.responsive.kafka.internal.metrics.StoreMetrics.TIME_SINCE_LAST_FLUSH_DESCRIPTION;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import dev.responsive.kafka.api.config.ResponsiveConfig;
import dev.responsive.kafka.internal.db.BytesKeySpec;
import dev.responsive.kafka.internal.db.CassandraClient;
import dev.responsive.kafka.internal.db.KeySpec;
import dev.responsive.kafka.internal.db.LwtWriterFactory;
import dev.responsive.kafka.internal.db.MetadataRow;
import dev.responsive.kafka.internal.db.RemoteKVTable;
import dev.responsive.kafka.internal.db.partitioning.SubPartitioner;
import dev.responsive.kafka.internal.db.spec.BaseTableSpec;
import dev.responsive.kafka.internal.metrics.ClientVersionMetadata;
import dev.responsive.kafka.internal.metrics.ResponsiveMetrics;
import dev.responsive.kafka.internal.utils.ExceptionSupplier;
import dev.responsive.kafka.internal.utils.SessionClients;
import dev.responsive.kafka.testutils.ResponsiveConfigParam;
import dev.responsive.kafka.testutils.ResponsiveExtension;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DeleteRecordsResult;
import org.apache.kafka.clients.admin.DeletedRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.CumulativeCount;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.Bytes;
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
 */

public class CommitBufferTest {
  /*
  private static final KeySpec<Bytes> KEY_SPEC = new BytesKeySpec();
  private static final Bytes KEY = Bytes.wrap(ByteBuffer.allocate(4).putInt(0).array());
  private static final byte[] VALUE = new byte[]{1};
  private static final int KAFKA_PARTITION = 2;
  private static final FlushTriggers TRIGGERS = FlushTriggers.ALWAYS;
  private static final ExceptionSupplier EXCEPTION_SUPPLIER = new ExceptionSupplier(true);
  private static final long CURRENT_TS = 100L;
  private static final long MIN_VALID_TS = 0L;

  private static final Map<String, String> APPLICATION_TAGS = Map.of(
      "responsive-version", "1",
      "responsive-commit-id", "abc",
      "streams-version", "2",
      "streams-commit-id", "dfe",
      "consumer-group", "commit-buffer-test-app",
      "streams-application-id", "commit-buffer-test-app",
      "streams-client-id", "commit-buffer-test-app-node-1"
  );

  @Mock
  private Admin admin;
  @Mock
  private Metrics metrics;
  @Mock
  private Sensor flushSensor;
  @Mock
  private Sensor flushLatencySensor;
  @Mock
  private Sensor flushErrorSensor;
  @Mock
  private Sensor failedTruncationsSensor;

  private Map<String, String> storeTags;
  private CqlSession session;
  private SessionClients sessionClients;
  private TopicPartition changelogTp;
  private String name;
  private SubPartitioner partitioner;
  private RemoteKVTable<BoundStatement> table;
  private CassandraClient cclient;

  @BeforeEach
  public void before(
      final TestInfo info,
      final CassandraContainer<?> cassandra,
      @ResponsiveConfigParam final ResponsiveConfig config
  ) throws InterruptedException, TimeoutException {
    name = info.getTestMethod().orElseThrow().getName();
    session = CqlSession.builder()
        .addContactPoint(cassandra.getContactPoint())
        .withLocalDatacenter(cassandra.getLocalDatacenter())
        .withKeyspace("responsive_clients") // NOTE: this keyspace is expected to exist
        .build();
    cclient = new CassandraClient(session, config);
    sessionClients = new SessionClients(
        Optional.empty(),
        Optional.of(cclient),
        admin
    );
    final var responsiveMetrics = new ResponsiveMetrics(metrics);
    responsiveMetrics.initializeTags(
        "commit-buffer-test-app",
        "commit-buffer-test-app-node-1",
        new ClientVersionMetadata("1", "abc", "2", "dfe"),
        Collections.emptyMap()
    );
    partitioner = SubPartitioner.NO_SUBPARTITIONS;
    sessionClients.initialize(responsiveMetrics, null);
    table = cclient.kvFactory().create(new BaseTableSpec(name, partitioner));
    changelogTp = new TopicPartition(name + "-changelog", KAFKA_PARTITION);

    when(admin.deleteRecords(Mockito.any())).thenReturn(new DeleteRecordsResult(Map.of(
            changelogTp, KafkaFuture.completedFuture(new DeletedRecords(100)))));

    storeTags = new HashMap<>(APPLICATION_TAGS);
    storeTags.put("thread-id", Thread.currentThread().getName());
    storeTags.put("topic", changelogTp.topic());
    storeTags.put("partition", Integer.toString(changelogTp.partition()));
    storeTags.put("store", name);

    Mockito.when(metrics.sensor("flush-" + changelogTp)).thenReturn(flushSensor);
    Mockito.when(metrics.sensor("flush-latency-" + changelogTp)).thenReturn(flushLatencySensor);
    Mockito.when(metrics.sensor("flush-errors-" + changelogTp)).thenReturn(flushErrorSensor);
    Mockito.when(metrics.sensor("failed-truncations-" + changelogTp))
        .thenReturn(failedTruncationsSensor);
  }

  private MetricName metricName(final String name, final String description) {
    return new MetricName(
        name,
        "store-metrics",
        description,
        storeTags
    );
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
  public void shouldAddAllBufferMetrics() {
    // Given:
    setPartitioner(3);

    // When:
    final CommitBuffer<Bytes, RemoteKVTable<BoundStatement>> buffer = new CommitBuffer<>(
        sessionClients, changelogTp, admin, table,
        KEY_SPEC, true, name, TRIGGERS, EXCEPTION_SUPPLIER, partitioner);

    // Then:
    Mockito.verify(metrics).addMetric(
        eq(metricName(TIME_SINCE_LAST_FLUSH, TIME_SINCE_LAST_FLUSH_DESCRIPTION)),
        any(Gauge.class));

    Mockito.verify(flushSensor).add(
        eq(metricName(FLUSH_RATE, FLUSH_RATE_DESCRIPTION)),
        any(Rate.class));
    Mockito.verify(flushSensor).add(
        eq(metricName(FLUSH_TOTAL, FLUSH_TOTAL_DESCRIPTION)),
        any(CumulativeCount.class));

    Mockito.verify(flushLatencySensor).add(
        eq(metricName(FLUSH_LATENCY_AVG, FLUSH_LATENCY_AVG_DESCRIPTION)),
        any(Avg.class));
    Mockito.verify(flushLatencySensor).add(
        eq(metricName(FLUSH_LATENCY_MAX, FLUSH_LATENCY_MAX_DESCRIPTION)),
        any(Max.class));

    Mockito.verify(flushErrorSensor).add(
        eq(metricName(FLUSH_ERRORS_RATE, FLUSH_ERRORS_RATE_DESCRIPTION)),
        any(Rate.class));
    Mockito.verify(flushErrorSensor).add(
        eq(metricName(FLUSH_ERRORS_TOTAL, FLUSH_ERRORS_TOTAL_DESCRIPTION)),
        any(CumulativeCount.class));

    Mockito.verify(failedTruncationsSensor).add(
        eq(metricName(FAILED_TRUNCATIONS_RATE, FAILED_TRUNCATIONS_RATE_DESCRIPTION)),
        any(Rate.class));
    Mockito.verify(failedTruncationsSensor).add(
        eq(metricName(FAILED_TRUNCATIONS_TOTAL, FAILED_TRUNCATIONS_TOTAL_DESCRIPTION)),
        any(CumulativeCount.class));
  }

  @Test
  public void shouldRemoveAllBufferMetrics() {
    // Given:
    setPartitioner(3);
    final CommitBuffer<Bytes, RemoteKVTable<BoundStatement>> buffer = new CommitBuffer<>(
        sessionClients, changelogTp, admin, table,
        KEY_SPEC, true, name, TRIGGERS, EXCEPTION_SUPPLIER, partitioner);
    buffer.init();

    // When:
    buffer.close();

    // Then:
    Mockito.verify(metrics).removeMetric(
        eq(metricName(TIME_SINCE_LAST_FLUSH, TIME_SINCE_LAST_FLUSH_DESCRIPTION)));

    Mockito.verify(metrics).removeSensor("flush-" + changelogTp);
    Mockito.verify(metrics).removeSensor("flush-latency-" + changelogTp);
    Mockito.verify(metrics).removeSensor("flush-errors-" + changelogTp);
    Mockito.verify(metrics).removeSensor("failed-truncations-" + changelogTp);
  }

  @Test
  public void shouldFlushWithCorrectEpochForPartitionsWithDataAndOffsetForBaseSubpartition() {
    // Given:
    setPartitioner(3);
    final CommitBuffer<Bytes, RemoteKVTable<BoundStatement>> buffer = new CommitBuffer<>(
        sessionClients, changelogTp, admin, table,
        KEY_SPEC, true, name, TRIGGERS, EXCEPTION_SUPPLIER, partitioner);
    buffer.init();

    // reserve epoch for partition 8 to ensure it doesn't get flushed
    // if it did it would get fenced
    LwtWriterFactory.initialize(table, table, cclient, new int[]{8}, KAFKA_PARTITION, 3L, false);

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
    assertThat(table.get(6, k0, MIN_VALID_TS), is(VALUE));
    assertThat(table.get(7, k1, MIN_VALID_TS), is(VALUE));
    assertThat(table.fetchOffset(6), is(new MetadataRow(100L, 1L)));
    assertThat(table.fetchOffset(7), is(new MetadataRow(-1L, 1L)));
    assertThat(table.fetchOffset(8), is(new MetadataRow(-1L, 3L)));
  }

  @Test
  public void shouldNotFlushWithExpiredEpoch() {
    // Given:
    final ExceptionSupplier exceptionSupplier = mock(ExceptionSupplier.class);
    final CommitBuffer<Bytes, RemoteKVTable<BoundStatement>> buffer = new CommitBuffer<>(
        sessionClients, changelogTp, admin, table,
        KEY_SPEC, true, name, TRIGGERS, exceptionSupplier, partitioner);
    buffer.init();

    LwtWriterFactory.initialize(
        table, cclient, new int[]{KAFKA_PARTITION}, KAFKA_PARTITION, 100L, false);


    Mockito.when(exceptionSupplier.commitFencedException(anyString()))
        .thenAnswer(msg -> new RuntimeException(msg.getArgument(0).toString()));

    // When:
    final var e = assertThrows(
        RuntimeException.class,
        () -> {
          buffer.put(KEY, VALUE, CURRENT_TS);
          buffer.flush(100L);
        }
    );

    // Then:
    final String errorMsg = "commit-buffer [" + table.name() + "-2] "
        + "[2] Error while writing batch! Local Epoch: LwtWriterFactory{epoch=1}, "
        + "Persisted Epoch: 100, Batch Offset: 100, Persisted Offset: -1";
    assertThat(e.getMessage(), equalTo(errorMsg));
  }

  @Test
  public void shouldIncrementEpochAndReserveForAllSubpartitionsOnInit() {
    // Given:
    setPartitioner(2);
    final CommitBuffer<Bytes, RemoteKVTable<BoundStatement>> buffer = new CommitBuffer<>(
        sessionClients, changelogTp, admin, table,
        KEY_SPEC, true, name, TRIGGERS, EXCEPTION_SUPPLIER, partitioner);
    // throwaway init to initialize table
    buffer.init();

    LwtWriterFactory.initialize(table, cclient, new int[]{4}, KAFKA_PARTITION, 100L, false);
    LwtWriterFactory.initialize(table, cclient, new int[]{5}, KAFKA_PARTITION, 100L, false);

    // When:
    buffer.init();

    // Then:
    assertThat(table.fetchOffset(4).epoch, is(101L));
    assertThat(table.fetchOffset(5).epoch, is(101L));
  }

  @Test
  public void shouldTruncateTopicAfterFlushIfTruncateEnabled() {
    // Given:
    final CommitBuffer<Bytes, RemoteKVTable<BoundStatement>> buffer = new CommitBuffer<>(
        sessionClients, changelogTp, admin, table,
        KEY_SPEC, true, name, TRIGGERS, EXCEPTION_SUPPLIER, partitioner);
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
    final CommitBuffer<Bytes, RemoteKVTable<BoundStatement>> buffer = new CommitBuffer<>(
        sessionClients, changelogTp, admin, table,
        KEY_SPEC, false, name, TRIGGERS, EXCEPTION_SUPPLIER, partitioner);
    buffer.init();
    buffer.put(KEY, VALUE, CURRENT_TS);

    // when:
    buffer.flush(100L);

    // Then:
    verifyNoInteractions(admin);
  }

  @Test
  public void shouldNotTruncateTopicAfterFlushIfTruncateEnabledForSourceTopicChangelog() {
    // Given:
    final var sourceChangelog = new TopicPartition("some-source-topic", KAFKA_PARTITION);
    Mockito.when(metrics.sensor("flush-" + sourceChangelog))
        .thenReturn(flushSensor);
    Mockito.when(metrics.sensor("flush-latency-" + sourceChangelog))
        .thenReturn(flushLatencySensor);
    Mockito.when(metrics.sensor("flush-errors-" + sourceChangelog))
        .thenReturn(flushErrorSensor);
    Mockito.when(metrics.sensor("failed-truncations-" + sourceChangelog))
        .thenReturn(failedTruncationsSensor);
    final CommitBuffer<Bytes, RemoteKVTable<BoundStatement>> buffer = new CommitBuffer<>(
        sessionClients,
        sourceChangelog,
        admin,
        table,
        KEY_SPEC,
        true,
        name,
        TRIGGERS,
        EXCEPTION_SUPPLIER,
        partitioner
    );
    buffer.init();
    buffer.put(KEY, VALUE, CURRENT_TS);

    // when:
    buffer.flush(100L);

    // Then:
    verifyNoInteractions(admin);
  }

  @Test
  public void shouldNotTruncateTopicAfterPolicyViolationException() {
    // Given:
    final CommitBuffer<Bytes, RemoteKVTable<BoundStatement>> buffer = new CommitBuffer<>(
        sessionClients, changelogTp, admin, table,
        KEY_SPEC, true, name, TRIGGERS, EXCEPTION_SUPPLIER, partitioner);
    buffer.init();
    buffer.put(KEY, VALUE, CURRENT_TS);

    final KafkaFutureImpl<DeletedRecords> future = new KafkaFutureImpl<>();
    future.completeExceptionally(new PolicyViolationException("oops"));
    when(admin.deleteRecords(any())).thenReturn(new DeleteRecordsResult(Collections.singletonMap(
        changelogTp,
        future
    )));

    // When:
    buffer.flush(100L);
    verify(admin).deleteRecords(any());
    buffer.put(KEY, VALUE, CURRENT_TS + 50_000);
    buffer.flush(101L);

    // Then:
    verifyNoMoreInteractions(admin);
  }

  @Test
  public void shouldOnlyFlushWhenBufferFullWithRecordsTrigger() {
    // Given:
    final CommitBuffer<Bytes, RemoteKVTable<BoundStatement>> buffer = new CommitBuffer<>(
        sessionClients,
        changelogTp,
        admin,
        table,
        KEY_SPEC,
        false,
        name,
        FlushTriggers.ofRecords(10),
        EXCEPTION_SUPPLIER,
        partitioner
    );
    buffer.init();

    for (byte i = 0; i < 9; i++) {
      buffer.put(Bytes.wrap(new byte[]{i}), VALUE, CURRENT_TS);
    }

    // when:
    buffer.flush(9L);

    // Then:
    assertThat(table.fetchOffset(KAFKA_PARTITION).offset, is(-1L));
    buffer.put(Bytes.wrap(new byte[]{10}), VALUE, CURRENT_TS);
    buffer.flush(10L);
    assertThat(table.fetchOffset(KAFKA_PARTITION).offset, is(10L));
  }

  @Test
  public void shouldOnlyFlushWhenBufferFullWithBytesTrigger() {
    // Given:
    final CommitBuffer<Bytes, RemoteKVTable<BoundStatement>> buffer = new CommitBuffer<>(
        sessionClients,
        changelogTp,
        admin,
        table,
        KEY_SPEC,
        false,
        name,
        FlushTriggers.ofBytes(170),
        EXCEPTION_SUPPLIER,
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
    assertThat(table.fetchOffset(KAFKA_PARTITION).offset, is(-1L));
    buffer.put(Bytes.wrap(new byte[]{10}), value, CURRENT_TS);
    buffer.flush(10L);
    assertThat(table.fetchOffset(KAFKA_PARTITION).offset, is(10L));
  }

  @Test
  public void shouldOnlyFlushWhenIntervalTriggerElapsed() {
    // Given:
    // just use an atomic reference as a mutable reference type
    final AtomicReference<Instant> clock = new AtomicReference<>(Instant.now());
    final CommitBuffer<Bytes, RemoteKVTable<BoundStatement>> buffer = new CommitBuffer<>(
        sessionClients,
        changelogTp,
        admin,
        table,
        KEY_SPEC,
        false,
        name,
        FlushTriggers.ofInterval(Duration.ofSeconds(30)),
        EXCEPTION_SUPPLIER,
        100,
        partitioner,
        clock::get
    );
    buffer.init();
    buffer.put(Bytes.wrap(new byte[]{18}), VALUE, CURRENT_TS);

    // When:
    buffer.flush(1L);

    // Then:
    assertThat(table.fetchOffset(KAFKA_PARTITION).offset, is(-1L));
    clock.set(clock.get().plus(Duration.ofSeconds(35)));
    buffer.flush(5L);
    assertThat(table.fetchOffset(KAFKA_PARTITION).offset, is(5L));
  }

  @Test
  public void shouldDeleteRowInCassandraWithTombstone() {
    // Given:
    cclient.execute(this.table.insert(KAFKA_PARTITION, KEY, VALUE, CURRENT_TS));
    final CommitBuffer<Bytes, RemoteKVTable<BoundStatement>> buffer = new CommitBuffer<>(
        sessionClients, changelogTp, admin, this.table,
        KEY_SPEC, true, name, TRIGGERS, EXCEPTION_SUPPLIER, partitioner);
    buffer.init();

    // When:
    buffer.tombstone(KEY, CURRENT_TS);
    buffer.flush(100L);

    // Then:
    final byte[] value = this.table.get(KAFKA_PARTITION, KEY, CURRENT_TS);
    assertThat(value, nullValue());
  }

  @Test
  public void shouldRestoreRecords() {
    // Given:
    final CommitBuffer<Bytes, RemoteKVTable<BoundStatement>> buffer = new CommitBuffer<>(
        sessionClients, changelogTp, admin, table,
        KEY_SPEC, true, name, TRIGGERS, EXCEPTION_SUPPLIER, partitioner);
    buffer.init();

    final ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(
        changelogTp.topic(),
        changelogTp.partition(),
        100,
        CURRENT_TS,
        TimestampType.CREATE_TIME,
        KEY.get().length,
        VALUE.length,
        KEY.get(),
        VALUE,
        new RecordHeaders(),
        Optional.empty()
    );

    // When:
    buffer.restoreBatch(List.of(record));

    // Then:
    assertThat(table.fetchOffset(KAFKA_PARTITION).offset, is(100L));
    final byte[] value = table.get(KAFKA_PARTITION, KEY, MIN_VALID_TS);
    assertThat(value, is(VALUE));
  }

  @Test
  public void shouldNotRestoreRecordsWhenFencedByEpoch() {
    // Given:
    final ExceptionSupplier exceptionSupplier = mock(ExceptionSupplier.class);
    final CommitBuffer<Bytes, RemoteKVTable<BoundStatement>> buffer = new CommitBuffer<>(
        sessionClients, changelogTp, admin, table,
        KEY_SPEC, true, name, TRIGGERS, exceptionSupplier, partitioner);
    buffer.init();
    LwtWriterFactory.initialize(
        table, cclient, new int[]{KAFKA_PARTITION}, KAFKA_PARTITION, 100L, false);

    final ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(
        changelogTp.topic(),
        KAFKA_PARTITION,
        100,
        CURRENT_TS,
        TimestampType.CREATE_TIME,
        KEY.get().length,
        VALUE.length,
        KEY.get(),
        VALUE,
        new RecordHeaders(),
        Optional.empty()
    );

    Mockito.when(exceptionSupplier.commitFencedException(anyString()))
        .thenAnswer(msg -> new RuntimeException(msg.getArgument(0).toString()));

    // When:
    final var e = assertThrows(
        RuntimeException.class,
        () -> buffer.restoreBatch(List.of(record)));

    // Then:
    final String errorMsg = "commit-buffer [" + table.name() + "-2] "
        + "[2] Error while writing batch! Local Epoch: LwtWriterFactory{epoch=1}, "
        + "Persisted Epoch: 100, Batch Offset: 100, Persisted Offset: -1";
    assertThat(e.getMessage(), equalTo(errorMsg));
  }

  @Test
  public void shouldRestoreStreamsBatchLargerThanCassandraBatch() {
    // Given:
    final CommitBuffer<Bytes, RemoteKVTable<BoundStatement>> buffer = new CommitBuffer<>(
        sessionClients,
        changelogTp,
        admin,
        table,
        KEY_SPEC,
        true,
        name,
        TRIGGERS,
        EXCEPTION_SUPPLIER,
        3,
        partitioner,
        Instant::now
    );
    buffer.init();
    cclient.execute(table.setOffset(100, 1));

    final List<ConsumerRecord<byte[], byte[]>> records = IntStream.range(0, 5)
        .mapToObj(i -> new ConsumerRecord<>(
            changelogTp.topic(),
            changelogTp.partition(),
            101L + i,
            CURRENT_TS,
            TimestampType.CREATE_TIME,
            8,
            8,
            Integer.toString(i).getBytes(Charset.defaultCharset()),
            Integer.toString(i).getBytes(Charset.defaultCharset()),
            new RecordHeaders(),
            Optional.empty()))
        .collect(Collectors.toList());

    // when:
    buffer.restoreBatch(records);

    // then:
    for (int i = 0; i < 5; i++) {
      assertThat(
          table.get(
              KAFKA_PARTITION,
              Bytes.wrap(Integer.toString(i).getBytes(Charset.defaultCharset())),
              MIN_VALID_TS),
          is(Integer.toString(i).getBytes(Charset.defaultCharset())));
    }
  }
*/
}
