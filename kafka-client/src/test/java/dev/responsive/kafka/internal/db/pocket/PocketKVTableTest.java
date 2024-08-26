package dev.responsive.kafka.internal.db.pocket;

import static dev.responsive.kafka.testutils.ResponsiveExtension.PocketContainer;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import dev.responsive.kafka.api.config.ResponsiveConfig;
import dev.responsive.kafka.api.config.StorageBackend;
import dev.responsive.kafka.internal.db.pocket.client.LssId;
import dev.responsive.kafka.internal.db.pocket.client.PocketClient;
import dev.responsive.kafka.internal.db.pocket.client.Put;
import dev.responsive.kafka.internal.db.pocket.client.grpc.GrpcPocketClient;
import dev.responsive.kafka.internal.stores.ResponsiveStoreRegistration;
import dev.responsive.kafka.testutils.ResponsiveConfigParam;
import dev.responsive.kafka.testutils.ResponsiveExtension;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import junit.framework.AssertionFailedError;
import org.apache.kafka.common.utils.Bytes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.RegisterExtension;

public class PocketKVTableTest {
  private static final UUID STORE_ID = new UUID(0, 0);
  private static final int PARTITION_ID = 1;

  private String testName;
  private PocketClient pocketClient;
  private final PssPartitioner pssPartitioner = PssRangePartitioner.create(
      List.of("000", "001", "010", "011", "100", "101", "110", "111")
  );

  @RegisterExtension
  public static final ResponsiveExtension EXT = new ResponsiveExtension(StorageBackend.POCKET);

  private PocketKVTable table;
  private PocketContainer pocketContainer;

  @BeforeEach
  public void setup(
      final TestInfo info,
      final PocketContainer pocketContainer,
      @ResponsiveConfigParam final ResponsiveConfig responsiveConfig
  ) {
    testName = info.getTestMethod().orElseThrow().getName();
    final int port = pocketContainer.getMappedPort(50051);
    this.pocketContainer = pocketContainer;
    pocketClient = GrpcPocketClient.connect(String.format("localhost:%d", port));
    this.table = new PocketKVTable(
        testName,
        STORE_ID,
        pocketClient,
        pssPartitioner
    );
  }

  @AfterEach
  public void teardown() {
    System.out.println(pocketContainer.getLogs());
    pocketClient.close();
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
  public void shouldWriteToPocketStore() throws InterruptedException, ExecutionException {
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
    final var result = pocketClient.get(
        STORE_ID,
        new LssId(PARTITION_ID),
        pss,
        Optional.of(10L),
        key.get()
    );
    assertThat(result.get(), is("bar".getBytes()));
  }

  @Test
  public void shouldRestoreFromLowestPssFlushedOffset() {
    // given:
    int endOffset = 100;
    for (final int pssId : pssPartitioner.allPss()) {
      pocketClient.writeWalSegment(
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
    for (final int pssId : pssPartitioner.allPss()) {
      runTimeout(
          () -> {
            final var offsets
                = pocketClient.getCurrentOffsets(STORE_ID, new LssId(PARTITION_ID), pssId);
            offsets.flushedOffset().orElseThrow(() -> new RuntimeException("not flushed"));
          },
          Duration.ofMinutes(5)
      );
    }
    table.init(PARTITION_ID);

    // when:
    final var restorePartition = table.fetchOffset(PARTITION_ID);
    assertThat(restorePartition, is(100L));
  }

  @Test
  public void shouldRestoreFromStartIfLowestPssFlushedOffsetIsUnspecified() {
    int endOffset = 100;
    final Queue<Integer> allPssExcept1 = new LinkedList<>(pssPartitioner.allPss());
    allPssExcept1.remove();
    for (final int pssId : allPssExcept1) {
      pocketClient.writeWalSegment(
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
    for (final int pssId : allPssExcept1) {
      runTimeout(
          () -> {
            final var offsets
                = pocketClient.getCurrentOffsets(STORE_ID, new LssId(PARTITION_ID), pssId);
            offsets.flushedOffset().orElseThrow(() -> new RuntimeException("not flushed"));
          },
          Duration.ofMinutes(5)
      );
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
