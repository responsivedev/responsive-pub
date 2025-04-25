package dev.responsive.kafka.internal.db.rs3.client.grpc;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import dev.responsive.kafka.internal.db.rs3.client.LssId;
import dev.responsive.kafka.internal.db.rs3.client.Range;
import dev.responsive.kafka.internal.db.rs3.client.RangeBound;
import dev.responsive.kafka.internal.db.rs3.client.WalEntry;
import dev.responsive.kafka.internal.db.rs3.client.WindowedDelete;
import dev.responsive.kafka.internal.db.rs3.client.WindowedPut;
import dev.responsive.kafka.internal.utils.WindowedKey;
import dev.responsive.rs3.Rs3;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Stream;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class GrpcRS3ClientWindowEndToEndTest {
  private static final String SERVER_NAME = "localhost";
  private static final long RETRY_TIMEOUT_MS = 10000;
  private static final UUID STORE_ID = UUID.randomUUID();
  private static final int PSS_ID = 0;
  private static final LssId LSS_ID = new LssId(PSS_ID);

  private Server server;
  private ManagedChannel channel;
  private GrpcRS3Client client;

  @BeforeEach
  public void setUp() throws IOException {
    final var service = new TestGrpcRs3Service(
        STORE_ID,
        LSS_ID,
        PSS_ID,
        new WindowKeyValueStore()
    );
    this.server = InProcessServerBuilder
        .forName(SERVER_NAME)
        .addService(service)
        .build()
        .start();
    this.channel = Mockito.spy(InProcessChannelBuilder
                                   .forName(SERVER_NAME)
                                   .directExecutor()
                                   .build());
    this.client = new GrpcRS3Client(
        new PssStubsProvider(this.channel),
        Time.SYSTEM,
        RETRY_TIMEOUT_MS
    );
  }

  @AfterEach
  public void tearDown() {
    this.channel.shutdownNow();
    this.server.shutdownNow();
    this.client.close();
  }

  @Test
  public void shouldPutAndGet() {
    final var key = windowedKey("foo", 100L);
    final var value = "bar";
    writeWalSegment(
        10L,
        Collections.singletonList(windowedPut(key, value))
    );

    final var getResult = client.windowedGet(
        STORE_ID,
        LSS_ID,
        PSS_ID,
        Optional.of(5L),
        key
    );
    assertThat(getResult.isPresent(), is(true));
    final var resultValue = getResult.get();
    assertThat(new String(resultValue, StandardCharsets.UTF_8), equalTo(value));
  }


  @Test
  public void shouldDelete() {
    final var key = windowedKey("foo", 100L);
    writeWalSegment(
        10L,
        Collections.singletonList(windowedPut(key, "bar"))
    );

    assertThat(client.windowedGet(
        STORE_ID,
        LSS_ID,
        PSS_ID,
        Optional.of(5L),
        key
    ).isPresent(), is(true));

    writeWalSegment(
        10L,
        Collections.singletonList(windowedDelete(key))
    );

    assertThat(client.windowedGet(
        STORE_ID,
        LSS_ID,
        PSS_ID,
        Optional.of(5L),
        key
    ).isPresent(), is(false));
  }

  @Test
  public void shouldScanValuesInTimeWindowRange() {
    writeWalSegment(
        10L,
        Arrays.asList(
            windowedPut(windowedKey("a", 100L), "1"),
            windowedPut(windowedKey("b", 100L), "2"),
            windowedPut(windowedKey("a", 200L), "3"),
            windowedPut(windowedKey("b", 150L), "4"),
            windowedPut(windowedKey("c", 200L), "5"),
            windowedPut(windowedKey("a", 300L), "6")
        )
    );

    final var range = new Range<>(
        RangeBound.inclusive(windowedKey("a", 100L)),
        RangeBound.exclusive(windowedKey("a", 300L))
    );

    try (final var iter = client.windowedRange(
        STORE_ID,
        LSS_ID,
        PSS_ID,
        Optional.empty(),
        range
    )) {
      assertNext(iter, windowedKey("a", 100L), "1");
      assertNext(iter, windowedKey("a", 200L), "3");
      assertThat(iter.hasNext(), is(false));
    }
  }

  private void assertNext(
      KeyValueIterator<WindowedKey, byte[]> iter,
      WindowedKey key,
      String value
  ) {
    assertThat(iter.hasNext(), is(true));
    final var keyValue = iter.next();
    assertThat(keyValue.key, is(key));
    assertThat(Bytes.wrap(keyValue.value), is(Bytes.wrap(value.getBytes(StandardCharsets.UTF_8))));
  }

  private static WindowedKey windowedKey(String key, long windowTimestamp) {
    return new WindowedKey(
        key.getBytes(StandardCharsets.UTF_8),
        windowTimestamp
    );
  }

  private static WindowedPut windowedPut(
      WindowedKey key,
      String value
  ) {
    return new WindowedPut(
        key.key.get(),
        value.getBytes(StandardCharsets.UTF_8),
        0L,
        key.windowStartMs
    );
  }

  private static WindowedDelete windowedDelete(
      WindowedKey key
  ) {
    return new WindowedDelete(
        key.key.get(),
        key.windowStartMs
    );
  }

  private void writeWalSegment(long endOffset, List<WalEntry> entries) {
    final var sendRecv = client.writeWalSegmentAsync(
        STORE_ID,
        LSS_ID,
        PSS_ID,
        Optional.empty(),
        endOffset
    );

    entries.forEach(entry -> sendRecv.sender().sendNext(entry));
    sendRecv.sender().finish();

    final var flushedOffset = sendRecv
        .completion()
        .toCompletableFuture()
        .join();
    assertThat(flushedOffset, is(Optional.of(endOffset)));
  }

  static class WindowKeyValueStore implements TestGrpcRs3Service.KeyValueStore {
    private final ConcurrentSkipListMap<WindowedKey, Bytes> table = new ConcurrentSkipListMap<>();

    @Override
    public void put(final Rs3.KeyValue kvProto) {
      if (!kvProto.hasWindowKv()) {
        throw new UnsupportedOperationException("Unsupported kv type");
      }
      final var windowKv = kvProto.getWindowKv();
      final var windowKey = new WindowedKey(
          Bytes.wrap(windowKv.getKey().getKey().toByteArray()),
          windowKv.getKey().getWindowTimestamp()
      );
      final var valueBytes = Bytes.wrap(windowKv.getValue().getValue().toByteArray());
      table.put(windowKey, valueBytes);
    }

    @Override
    public void delete(final Rs3.Key keyProto) {
      if (!keyProto.hasWindowKey()) {
        throw new UnsupportedOperationException("Unsupported kv type");
      }
      final var windowKey = new WindowedKey(
          Bytes.wrap(keyProto.getWindowKey().getKey().toByteArray()),
          keyProto.getWindowKey().getWindowTimestamp()
      );
      table.remove(windowKey);
    }

    @Override
    public Optional<Rs3.KeyValue> get(final Rs3.Key keyProto) {
      if (!keyProto.hasWindowKey()) {
        throw new UnsupportedOperationException("Unsupported kv type");
      }
      final var windowKey = new WindowedKey(
          Bytes.wrap(keyProto.getWindowKey().getKey().toByteArray()),
          keyProto.getWindowKey().getWindowTimestamp()
      );

      final var valueBytes = table.get(windowKey);
      if (valueBytes == null) {
        return Optional.empty();
      } else {
        return Optional.of(
            Rs3.KeyValue.newBuilder()
                .setWindowKv(GrpcRs3Util.windowKeyValueProto(windowKey, valueBytes.get()))
                .build()
        );
      }
    }

    @Override
    public Stream<Rs3.KeyValue> range(final Rs3.Range rangeProto) {
      if (!rangeProto.hasWindowRange()) {
        throw new UnsupportedOperationException("Unsupported kv type");
      }

      final var windowRange = rangeProto.getWindowRange();
      final var range = new Range<>(
          decodeBound(windowRange.getFrom()),
          decodeBound(windowRange.getTo())
      );

      return table.entrySet().stream()
          .filter(entry -> range.contains(entry.getKey()))
          .map(entry -> Rs3.KeyValue.newBuilder().setWindowKv(
              GrpcRs3Util.windowKeyValueProto(entry.getKey(), entry.getValue().get())
          ).build());
    }

    private RangeBound<WindowedKey> decodeBound(Rs3.WindowBound bound) {
      if (bound.getType() == Rs3.BoundType.UNBOUNDED) {
        return RangeBound.unbounded();
      } else {
        final var windowKey = new WindowedKey(
            Bytes.wrap(bound.getKey().getKey().toByteArray()),
            bound.getKey().getWindowTimestamp()
        );
        if (bound.getType() == Rs3.BoundType.INCLUSIVE) {
          return RangeBound.inclusive(windowKey);
        } else if (bound.getType() == Rs3.BoundType.EXCLUSIVE) {
          return RangeBound.exclusive(windowKey);
        } else {
          throw new UnsupportedOperationException("Unsupported bound type");
        }
      }
    }
  }
}
