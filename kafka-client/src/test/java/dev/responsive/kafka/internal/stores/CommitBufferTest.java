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
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import dev.responsive.kafka.api.config.ResponsiveConfig;
import dev.responsive.kafka.internal.db.BatchFlusher;
import dev.responsive.kafka.internal.db.BytesKeySpec;
import dev.responsive.kafka.internal.db.CassandraClient;
import dev.responsive.kafka.internal.db.CassandraKeyValueTable;
import dev.responsive.kafka.internal.db.KeySpec;
import dev.responsive.kafka.internal.db.partitioning.SubPartitioner;
import dev.responsive.kafka.internal.db.spec.BaseTableSpec;
import dev.responsive.kafka.internal.metrics.ClientVersionMetadata;
import dev.responsive.kafka.internal.metrics.ResponsiveMetrics;
import dev.responsive.kafka.internal.utils.ExceptionSupplier;
import dev.responsive.kafka.internal.utils.SessionClients;
import dev.responsive.kafka.internal.utils.TableName;
import dev.responsive.kafka.testutils.ResponsiveConfigParam;
import dev.responsive.kafka.testutils.ResponsiveExtension;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DeleteRecordsResult;
import org.apache.kafka.clients.admin.DeletedRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.header.internals.RecordHeaders;
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

public class CommitBufferTest {

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

  private Map<String, String> storeTags;
  private CqlSession session;
  private SessionClients sessionClients;
  private TopicPartition changelog;
  private String name;
  private TableName tableName;
  private SubPartitioner partitioner;
  private CassandraKeyValueTable table;
  private CassandraClient client;
  private String threadName;

  @BeforeEach
  public void before(
      final TestInfo info,
      final CassandraContainer<?> cassandra,
      @ResponsiveConfigParam final ResponsiveConfig config
  ) throws InterruptedException, TimeoutException {
    name = info.getTestMethod().orElseThrow().getName();
    tableName = new TableName(name);
    session = CqlSession.builder()
        .addContactPoint(cassandra.getContactPoint())
        .withLocalDatacenter(cassandra.getLocalDatacenter())
        .withKeyspace("responsive_itests") // NOTE: this keyspace is expected to exist
        .build();
    client = new CassandraClient(session, config);
    sessionClients = new SessionClients(
        Optional.empty(),
        Optional.of(client),
        admin
    );
    final var responsiveMetrics = new ResponsiveMetrics(metrics);
    responsiveMetrics.initializeTags(
        "commit-buffer-test-app",
        "commit-buffer-test-app-node-1",
        new ClientVersionMetadata("1", "abc", "2", "dfe"),
        Collections.emptyMap()
    );
    partitioner = new SubPartitioner(
        4,
        k -> (int) k.get()[0]);

    sessionClients.initialize(responsiveMetrics, null);
    table = (CassandraKeyValueTable) client.kvFactory().create(
        new BaseTableSpec(name, partitioner));
    changelog = new TopicPartition(name + "-changelog", KAFKA_PARTITION);

    when(admin.deleteRecords(Mockito.any())).thenReturn(new DeleteRecordsResult(Map.of(
        changelog, KafkaFuture.completedFuture(new DeletedRecords(100)))));

    threadName = Thread.currentThread().getName();

    storeTags = new HashMap<>(APPLICATION_TAGS);
    storeTags.put("thread-id", threadName);
    storeTags.put("topic", changelog.topic());
    storeTags.put("partition", Integer.toString(changelog.partition()));
    storeTags.put("store", name);

    Mockito.when(metrics.sensor("flush-" + threadName + "-" + changelog))
        .thenReturn(flushSensor);
    Mockito.when(metrics.sensor("flush-latency-" + threadName + "-" + changelog))
        .thenReturn(flushLatencySensor);
    Mockito.when(metrics.sensor("flush-errors-" + threadName + "-" + changelog))
        .thenReturn(flushErrorSensor);
  }

  private CommitBuffer<Bytes, Integer> createCommitBuffer() {
    final var flushManager = table.init(changelog.partition());
    final BatchFlusher<Bytes, Integer> batchFlusher = new BatchFlusher<>(
        KEY_SPEC,
        changelog.partition(),
        flushManager
    );

    return new CommitBuffer<>(
        batchFlusher,
        sessionClients,
        changelog,
        admin,
        KEY_SPEC,
        tableName,
        false,
        TRIGGERS,
        EXCEPTION_SUPPLIER
    );
  }

  private CommitBuffer<Bytes, Integer> createCommitBuffer(
      final FlushTriggers flushTriggers,
      final int maxBatchSize,
      final Supplier<Instant> clock
  ) {
    return new CommitBuffer<>(
        new BatchFlusher<>(KEY_SPEC, changelog.partition(), table.init(KAFKA_PARTITION)),
        sessionClients,
        changelog,
        admin,
        KEY_SPEC,
        tableName,
        false,
        flushTriggers,
        EXCEPTION_SUPPLIER,
        maxBatchSize,
        clock
    );
  }

  private MetricName metricName(final String name, final String description) {
    return new MetricName(
        name,
        "store-metrics",
        description,
        storeTags
    );
  }

  @AfterEach
  public void after() {
    session.execute(SchemaBuilder.dropTable(name).build());
    session.close();
  }

  @Test
  public void shouldAddAllBufferMetrics() {
    // When:
    createCommitBuffer();

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
  }

  @Test
  public void shouldRemoveAllBufferMetrics() {
    // Given:
    final CommitBuffer<Bytes, Integer> buffer = createCommitBuffer();

    // When:
    buffer.close();

    // Then:
    Mockito.verify(metrics).removeMetric(
        eq(metricName(TIME_SINCE_LAST_FLUSH, TIME_SINCE_LAST_FLUSH_DESCRIPTION)));

    Mockito.verify(metrics).removeSensor("flush-" + threadName + "-" + changelog);
    Mockito.verify(metrics).removeSensor("flush-latency-" + threadName + "-" + changelog);
    Mockito.verify(metrics).removeSensor("flush-errors-" + threadName + "-" + changelog);
  }

  @Test
  public void shouldFenceOffsetFlushBasedOnMetadataRowEpoch() {
    // Given:
    try (final CommitBuffer<Bytes, Integer> buffer = createCommitBuffer()) {

      // reserve epoch 2 for partition 9 to ensure the flush attempt fails
      // to write for that partition
      client.execute(table.reserveEpoch(9, 2));

      final Bytes k1 = Bytes.wrap(new byte[]{1});
      final Bytes k2 = Bytes.wrap(new byte[]{2});

      // When:
      buffer.put(k1, VALUE, CURRENT_TS); // insert into subpartition 9
      buffer.put(k2, VALUE, CURRENT_TS); // insert into subpartition 10

      // flush with partial epoch fencing: only subpartition 9 has been bumped
      // so the offset update and k1 write will fail but k2 write will get through
      try {
        buffer.flush(100L);
      } catch (final ProducerFencedException expected) {

      }

      // Then:
      assertThat(table.get(changelog.partition(), k1, MIN_VALID_TS), nullValue());
      assertThat(table.get(changelog.partition(), k2, MIN_VALID_TS), is(VALUE));

      assertThat(table.fetchEpoch(8), is(1L));
      assertThat(table.fetchEpoch(9), is(2L));
      assertThat(table.fetchEpoch(10), is(1L));
      assertThat(table.fetchOffset(changelog.partition()), is(-1L));
    }
  }

  @Test
  public void shouldNotFlushWithExpiredEpoch() {
    // Given:
    final ExceptionSupplier exceptionSupplier = mock(ExceptionSupplier.class);
    final var flushManager = table.init(changelog.partition());
    try (final CommitBuffer<Bytes, Integer> buffer = new CommitBuffer<>(
        new BatchFlusher<>(KEY_SPEC, changelog.partition(), flushManager),
        sessionClients,
        changelog,
        admin,
        KEY_SPEC,
        tableName,
        false,
        TRIGGERS,
        exceptionSupplier)) {

      Mockito.when(exceptionSupplier.commitFencedException(anyString()))
          .thenAnswer(msg -> new RuntimeException(msg.getArgument(0).toString()));

      // init again to advance the epoch
      table.init(changelog.partition());

      // When:
      final var e = assertThrows(
          RuntimeException.class,
          () -> {
            buffer.put(KEY, VALUE, CURRENT_TS);
            buffer.flush(100L);
          }
      );

      // Then:
      final String errorMsg = "commit-buffer [" + tableName.tableName() + "-2] "
          + "Failed table partition [8]: "
          + "<batchOffset=100, persistedOffset=-1>, "
          + "<localEpoch=1, persistedEpoch=2>";
      assertThat(e.getMessage(), equalTo(errorMsg));
    }
  }

  @Test
  public void shouldIncrementEpochAndReserveForAllSubpartitionsOnInit() {
    // Given:
    final var flushManager = table.init(changelog.partition());
    new CommitBuffer<>(
        new BatchFlusher<>(KEY_SPEC, changelog.partition(), flushManager),
        sessionClients, changelog, admin,
        KEY_SPEC, tableName, false, TRIGGERS, EXCEPTION_SUPPLIER);

    for (final int tp : partitioner.allTablePartitions(changelog.partition())) {
      assertThat(table.fetchEpoch(tp), is(1L));
    }

    // When:
    table.init(changelog.partition());

    // Then:
    for (final int tp : partitioner.allTablePartitions(changelog.partition())) {
      assertThat(table.fetchEpoch(tp), is(2L));
    }
  }

  @Test
  public void shouldNotTruncateTopicAfterFlush() {
    // NOTE: this test is kept for historical purposes, we used to have a feature
    // that supported truncating changelog on flush. we no longer support this features
    // and this test ensures that

    // Given:
    try (final CommitBuffer<Bytes, Integer> buffer = createCommitBuffer()) {

      buffer.put(KEY, VALUE, CURRENT_TS);

      // when:
      buffer.flush(100L);

      // Then:
      verifyNoInteractions(admin);
    }
  }

  @Test
  public void shouldOnlyFlushWhenBufferFullWithRecordsTrigger() {
    // Given:
    try (final CommitBuffer<Bytes, Integer> buffer = createCommitBuffer(
        FlushTriggers.ofRecords(10),
        100,
        Instant::now
    )) {

      for (byte i = 0; i < 9; i++) {
        buffer.put(Bytes.wrap(new byte[]{i}), VALUE, CURRENT_TS);
      }

      // when:
      buffer.flush(9L);

      // Then:
      assertThat(table.fetchOffset(KAFKA_PARTITION), is(-1L));
      buffer.put(Bytes.wrap(new byte[]{10}), VALUE, CURRENT_TS);
      buffer.flush(10L);
      assertThat(table.fetchOffset(KAFKA_PARTITION), is(10L));
    }
  }

  @Test
  public void shouldOnlyFlushWhenBufferFullWithBytesTrigger() {
    // Given:
    try (final CommitBuffer<Bytes, Integer> buffer = createCommitBuffer(
        FlushTriggers.ofBytes(170),
        100,
        Instant::now
    )) {
      final byte[] value = new byte[9];
      for (byte i = 0; i < 9; i++) {
        buffer.put(Bytes.wrap(new byte[]{i}), value, CURRENT_TS);
      }

      // when:
      buffer.flush(9L);

      // Then:
      assertThat(table.fetchOffset(KAFKA_PARTITION), is(-1L));
      buffer.put(Bytes.wrap(new byte[]{10}), value, CURRENT_TS);
      buffer.flush(10L);
      assertThat(table.fetchOffset(KAFKA_PARTITION), is(10L));
    }
  }

  @Test
  public void shouldOnlyFlushWhenIntervalTriggerElapsed() {
    // Given:
    // just use an atomic reference as a mutable reference type
    final AtomicReference<Instant> clock = new AtomicReference<>(Instant.now());
    try (final CommitBuffer<Bytes, Integer> buffer = createCommitBuffer(
        FlushTriggers.ofInterval(Duration.ofSeconds(30)),
        100,
        clock::get)
    ) {
      buffer.put(Bytes.wrap(new byte[]{18}), VALUE, CURRENT_TS);

      // When:
      buffer.flush(1L);

      // Then:
      assertThat(table.fetchOffset(KAFKA_PARTITION), is(-1L));
      clock.set(clock.get().plus(Duration.ofSeconds(35)));
      buffer.flush(5L);
      assertThat(table.fetchOffset(KAFKA_PARTITION), is(5L));
    }
  }

  @Test
  public void shouldUpdateOffsetWhenNoRecordsInBuffer() {
    // Given:
    // just use an atomic reference as a mutable reference type
    final AtomicReference<Instant> clock = new AtomicReference<>(Instant.now());
    try (final CommitBuffer<Bytes, Integer> buffer = createCommitBuffer(
        FlushTriggers.ofInterval(Duration.ofSeconds(30)),
        100,
        clock::get
    )) {
      // when:
      clock.updateAndGet(old -> Instant.ofEpochSecond(old.getEpochSecond() + 32));
      buffer.flush(10L);

      // Then:
      assertThat(table.fetchOffset(KAFKA_PARTITION), is(10L));
    }
  }

  // measure how long before the commit buffer actually flushes the buffer (1-second granularity)
  // the measurement is taken by iterating until the flush succeeds, advancing the clock by
  // 1 second at each iteration.
  private Optional<Instant> measureFlushTime(
      final CommitBuffer<Bytes, Integer> buffer,
      final AtomicReference<Instant> clock,
      final long flushOffset,
      final Instant to
  ) {
    buffer.put(Bytes.wrap(new byte[]{18}), VALUE, CURRENT_TS);
    while (clock.get().isBefore(to)) {
      buffer.flush(flushOffset);
      if (table.fetchOffset(KAFKA_PARTITION) == flushOffset) {
        return Optional.of(clock.get());
      }
      clock.set(clock.get().plus(Duration.ofSeconds(1)));
    }
    return Optional.empty();
  }

  @Test
  public void shouldApplyJitterToFlushInterval() {
    // applies a 5-second jitter (for a 10 seconds range) and measures the flush interval
    // over 20 iterations. Then, the test asserts that we got at least 2 different intervals.
    // The test has a (1/10 ^ 20) probability of returning a false-negative
    final AtomicReference<Instant> clock = new AtomicReference<>(Instant.now());
    try (final CommitBuffer<Bytes, Integer> buffer = createCommitBuffer(
        FlushTriggers.ofInterval(Duration.ofSeconds(20), Duration.ofSeconds(5)),
        100,
        clock::get)
    ) {
      // given:
      final Set<Duration> intervals = new HashSet<>();
      for (int i = 0; i < 20; i++) {
        final Instant start = clock.get();

        // when:
        final Optional<Instant> flushTime
            = measureFlushTime(buffer, clock, 5 + i, start.plus(Duration.ofSeconds(26)));

        // then make sure the flush happened in a time that's in range:
        assertThat(flushTime.isPresent(), is(true));
        final Duration flushInterval = Duration.between(start, flushTime.get());
        final long flushIntervalSeconds = flushInterval.getSeconds();
        assertThat(flushIntervalSeconds, greaterThanOrEqualTo(15L));
        intervals.add(flushInterval);
      }
      // then make sure that there was some randomness to the jitter:
      assertThat(intervals.size(), greaterThan(1));
    }
  }

  @Test
  public void shouldDeleteRowInCassandraWithTombstone() {
    // Given:
    client.execute(this.table.insert(KAFKA_PARTITION, KEY, VALUE, CURRENT_TS));
    try (final CommitBuffer<Bytes, Integer> buffer = createCommitBuffer()) {

      // When:
      buffer.tombstone(KEY, CURRENT_TS);
      buffer.flush(100L);

      // Then:
      final byte[] value = this.table.get(KAFKA_PARTITION, KEY, CURRENT_TS);
      assertThat(value, nullValue());
    }
  }

  @Test
  public void shouldRestoreRecords() {
    // Given:
    try (final CommitBuffer<Bytes, Integer> buffer = createCommitBuffer()) {

      final ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(
          changelog.topic(),
          changelog.partition(),
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
      assertThat(table.fetchOffset(KAFKA_PARTITION), is(100L));
      final byte[] value = table.get(KAFKA_PARTITION, KEY, MIN_VALID_TS);
      assertThat(value, is(VALUE));
    }
  }

  @Test
  public void shouldNotRestoreRecordsWhenFencedByEpoch() {
    // Given:
    final ExceptionSupplier exceptionSupplier = mock(ExceptionSupplier.class);
    final var flushManager = table.init(changelog.partition());
    final CommitBuffer<Bytes, Integer> buffer = new CommitBuffer<>(
        new BatchFlusher<>(KEY_SPEC, changelog.partition(), flushManager),
        sessionClients, changelog, admin,
        KEY_SPEC, tableName, false, TRIGGERS, exceptionSupplier);

    // initialize a new writer to bump the epoch
    table.init(changelog.partition());

    final ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(
        changelog.topic(),
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
    final String errorMsg = "commit-buffer [" + tableName.tableName() + "-2] "
        + "Failed table partition [8]: "
        + "<batchOffset=100, persistedOffset=-1>, "
        + "<localEpoch=1, persistedEpoch=2>";
    assertThat(e.getMessage(), equalTo(errorMsg));
  }

  @Test
  public void shouldRestoreStreamsBatchLargerThanCassandraBatch() {
    // Given:
    final var flushManager = table.init(changelog.partition());
    final CommitBuffer<Bytes, Integer> buffer = new CommitBuffer<>(
        new BatchFlusher<>(KEY_SPEC, changelog.partition(), flushManager),
        sessionClients,
        changelog,
        admin,
        KEY_SPEC,
        tableName,
        false,
        TRIGGERS,
        EXCEPTION_SUPPLIER,
        3,
        Instant::now
    );
    client.execute(table.setOffset(100, 1));

    final List<ConsumerRecord<byte[], byte[]>> records = IntStream.range(0, 5)
        .mapToObj(i -> new ConsumerRecord<>(
            changelog.topic(),
            changelog.partition(),
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

}
