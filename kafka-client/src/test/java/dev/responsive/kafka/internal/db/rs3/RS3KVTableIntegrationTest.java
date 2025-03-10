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

package dev.responsive.kafka.internal.db.rs3;

import static dev.responsive.kafka.testutils.ResponsiveExtension.RS3Container;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import dev.responsive.kafka.api.config.ResponsiveConfig;
import dev.responsive.kafka.api.config.StorageBackend;
import dev.responsive.kafka.internal.db.rs3.client.LssId;
import dev.responsive.kafka.internal.db.rs3.client.Put;
import dev.responsive.kafka.internal.db.rs3.client.RS3Client;
import dev.responsive.kafka.internal.db.rs3.client.grpc.GrpcRS3Client;
import dev.responsive.kafka.internal.metrics.ClientVersionMetadata;
import dev.responsive.kafka.internal.metrics.ResponsiveMetrics;
import dev.responsive.kafka.internal.stores.ResponsiveStoreRegistration;
import dev.responsive.kafka.testutils.ResponsiveConfigParam;
import dev.responsive.kafka.testutils.ResponsiveExtension;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import junit.framework.AssertionFailedError;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.MockTime;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.RegisterExtension;

public class RS3KVTableIntegrationTest {
  private static final UUID STORE_ID = new UUID(0, 0);
  private static final int PARTITION_ID = 8;

  private String testName;
  private RS3Client rs3Client;
  private final PssPartitioner pssPartitioner = new PssDirectPartitioner();
  private final Metrics metrics = new Metrics();
  private final long retryTimeoutMs = 30000;
  private final MockTime time = new MockTime();

  @RegisterExtension
  public static final ResponsiveExtension EXT = new ResponsiveExtension(StorageBackend.RS3);

  private RS3KVTable table;
  private RS3Container rs3Container;

  @BeforeEach
  public void setup(
      final TestInfo info,
      final RS3Container rs3Container,
      @ResponsiveConfigParam final ResponsiveConfig responsiveConfig
  ) {
    testName = info.getTestMethod().orElseThrow().getName();
    final int port = rs3Container.getMappedPort(50051);
    this.rs3Container = rs3Container;
    final GrpcRS3Client.Connector connector =
        new GrpcRS3Client.Connector(time, "localhost", port);
    connector.useTls(false);
    rs3Client = connector.connect();
    final ResponsiveMetrics responsiveMetrics = new ResponsiveMetrics(metrics);
    responsiveMetrics.initializeTags(
        "application-id",
        "streams-client-id",
        new ClientVersionMetadata(
            "responsive-client-version",
            "responsive-client-commit-id",
            "streams-client-version",
            "streams-client-commit-id"
        ),
        Map.of()
    );
    final ResponsiveMetrics.MetricScopeBuilder scopeBuilder
        = responsiveMetrics.storeLevelMetricScopeBuilder(
            "thread-id",
            new TopicPartition("foo", 0),
            "store-name"
    );
    this.table = new RS3KVTable(
        testName,
        STORE_ID,
        rs3Client,
        pssPartitioner,
        responsiveMetrics,
        scopeBuilder
    );
  }

  @AfterEach
  public void teardown() {
    System.out.println(rs3Container.getLogs());
    rs3Client.close();
  }

  @Test
  public void shouldReadWriteFromKVStore() throws InterruptedException, ExecutionException {
    // given:
    final var flushManager = table.init(PARTITION_ID);
    final var tablePartitioner = flushManager.partitioner();
    final var key = Bytes.wrap("foo".getBytes());
    final int pss = tablePartitioner.tablePartition(PARTITION_ID, key);

    // when:
    flushManager.preFlush();
    final var writer = flushManager.createWriter(pss, 10);
    writer.insert(key, "bar".getBytes(), 100);
    writer.flush().toCompletableFuture().get();
    flushManager.postFlush(10);
    final var result = table.get(PARTITION_ID, key, 0);

    // then:
    assertThat(result, is("bar".getBytes()));
  }

  @Test
  public void shouldWriteToStore() throws InterruptedException, ExecutionException {
    // given:
    final var flushManager = table.init(PARTITION_ID);
    final var tablePartitioner = flushManager.partitioner();
    final var key = Bytes.wrap("foo".getBytes());
    final int pss = tablePartitioner.tablePartition(PARTITION_ID, key);

    // when:
    flushManager.preFlush();
    final var writer = flushManager.createWriter(pss, 10);
    writer.insert(key, "bar".getBytes(), 100);
    writer.flush().toCompletableFuture().get();
    flushManager.postFlush(10);

    // then:
    final var result = rs3Client.get(
        STORE_ID,
        new LssId(PARTITION_ID),
        pss,
        Optional.of(10L),
        key.get()
    );
    assertThat(result.get(), is("bar".getBytes()));
  }

  @Test
  public void shouldRestoreFromLowestPssWrittenOffset() {
    // given:
    int endOffset = 100;
    for (final int pssId : pssPartitioner.pssForLss(new LssId(PARTITION_ID))) {
      rs3Client.writeWalSegment(
          STORE_ID,
          new LssId(PARTITION_ID),
          pssId,
          Optional.empty(),
          endOffset,
          List.of(
              new Put("foo".getBytes(), "bar".getBytes())
          )
      );
      endOffset += 1;
    }
    table.init(PARTITION_ID);

    // when:
    final var restorePartition = table.fetchOffset(PARTITION_ID);
    assertThat(restorePartition, is(100L));
  }

  @Test
  public void shouldRestoreFromStartIfLowestPssFlushedOffsetIsUnspecified() {
    int endOffset = 100;
    final Queue<Integer> allPssExcept1 = new LinkedList<>(
        pssPartitioner.pssForLss(new LssId(PARTITION_ID)));
    allPssExcept1.remove();
    for (final int pssId : allPssExcept1) {
      rs3Client.writeWalSegment(
          STORE_ID,
          new LssId(PARTITION_ID),
          pssId,
          Optional.empty(),
          endOffset,
          List.of(
              new Put("foo".getBytes(), "bar".getBytes())
          )
      );
      endOffset += 1;
    }
    table.init(PARTITION_ID);

    // when:
    final var restorePartition = table.fetchOffset(PARTITION_ID);
    assertThat(restorePartition, is(ResponsiveStoreRegistration.NO_COMMITTED_OFFSET));
  }

  private void runTimeout(final Runnable runnable, final Duration timeout) {
    final Instant start = Instant.now();
    while (start.plus(timeout).isAfter(Instant.now())) {
      try {
        runnable.run();
        return;
      } catch (final RuntimeException e) {
        try {
          Thread.sleep(100);
        } catch (final InterruptedException ee) {
          throw new RuntimeException(ee);
        }
      }
    }
    throw new AssertionFailedError("timed out waiting for condition");
  }
}
