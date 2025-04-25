package dev.responsive.kafka.internal.db.rs3.client.grpc;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

import dev.responsive.kafka.internal.db.rs3.client.LssId;
import dev.responsive.kafka.internal.db.rs3.client.Put;
import dev.responsive.kafka.internal.db.rs3.client.Range;
import dev.responsive.kafka.internal.db.rs3.client.RangeBound;
import dev.responsive.kafka.internal.db.rs3.client.WalEntry;
import dev.responsive.rs3.RS3Grpc;
import dev.responsive.rs3.Rs3;
import io.grpc.ClientCall;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
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

class GrpcRS3ClientBasicEndToEndTest {

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
        new BasicKeyValueStore()
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
    final var sendRecv = client.writeWalSegmentAsync(
        STORE_ID,
        LSS_ID,
        PSS_ID,
        Optional.empty(),
        5L
    );

    final var key = new byte[] { 0 };
    final var value = new byte[] { 1 };
    sendRecv.sender().sendNext(new Put(key, value));
    sendRecv.sender().finish();

    final var flushedOffset = sendRecv
        .completion()
        .toCompletableFuture()
        .join();
    assertThat(flushedOffset, is(Optional.of(5L)));

    final var getResult = client.get(
        STORE_ID,
        LSS_ID,
        PSS_ID,
        Optional.of(5L),
        Bytes.wrap(key)
    );
    assertThat(getResult.isPresent(), is(true));
    final var resultValue = getResult.get();
    assertThat(resultValue, equalTo(value));
  }

  @Test
  public void shouldScanAllKeyValues() {
    writeWalSegment(5L, Arrays.asList(
        buildPut("a", "foo"),
        buildPut("b", "bar"),
        buildPut("c", "baz")
    ));

    final var iter = client.range(
        STORE_ID,
        LSS_ID,
        PSS_ID,
        Optional.of(5L),
        new Range<Bytes>(
            RangeBound.unbounded(),
            RangeBound.unbounded()
        )
    );

    assertNext(iter, "a", "foo");
    assertNext(iter, "b", "bar");
    assertNext(iter, "c", "baz");
    assertThat(iter.hasNext(), is(false));
  }

  private void writeWalSegment(long endOffset, List<WalEntry> puts) {
    final var sendRecv = client.writeWalSegmentAsync(
        STORE_ID,
        LSS_ID,
        PSS_ID,
        Optional.empty(),
        endOffset
    );

    puts.forEach(put -> sendRecv.sender().sendNext(put));
    sendRecv.sender().finish();

    final var flushedOffset = sendRecv
        .completion()
        .toCompletableFuture()
        .join();
    assertThat(flushedOffset, is(Optional.of(endOffset)));
  }

  @Test
  public void shouldScanKeyValuesInBoundedRange() {
    writeWalSegment(10L, Arrays.asList(
        buildPut("a", "foo"),
        buildPut("b", "bar"),
        buildPut("c", "baz"),
        buildPut("d", "raz"),
        buildPut("e", "taz")
    ));

    final var iter = client.range(
        STORE_ID,
        LSS_ID,
        PSS_ID,
        Optional.of(10L),
        new Range<>(
            RangeBound.inclusive(Bytes.wrap("b".getBytes(StandardCharsets.UTF_8))),
            RangeBound.exclusive(Bytes.wrap("e".getBytes(StandardCharsets.UTF_8)))
        )
    );

    assertNext(iter, "b", "bar");
    assertNext(iter, "c", "baz");
    assertNext(iter, "d", "raz");
    assertThat(iter.hasNext(), is(false));
  }

  @Test
  public void shouldRetryRangeWithNetworkInterruption() {
    writeWalSegment(5L, Arrays.asList(
        buildPut("a", "foo"),
        buildPut("b", "bar"),
        buildPut("c", "baz")
    ));

    Mockito.doAnswer(invocation -> {
      @SuppressWarnings("unchecked")
      final var call = (ClientCall<Rs3.RangeRequest, Rs3.RangeResult>)
          invocation.callRealMethod();
      final var callSpy = Mockito.spy(call);
      Mockito.doThrow(new StatusRuntimeException(Status.UNAVAILABLE))
          .when(callSpy)
          .sendMessage(any());
      return callSpy;
    }).doCallRealMethod()
        .when(channel)
        .newCall(eq(RS3Grpc.getRangeMethod()), any());

    final var iter = client.range(
        STORE_ID,
        LSS_ID,
        PSS_ID,
        Optional.of(5L),
        new Range<Bytes>(
            RangeBound.unbounded(),
            RangeBound.unbounded()
        )
    );

    assertNext(iter, "a", "foo");
    assertNext(iter, "b", "bar");
    assertNext(iter, "c", "baz");
    assertThat(iter.hasNext(), is(false));
  }

  @Test
  public void shouldRetryPutWithNetworkInterruption() {
    Mockito.doAnswer(invocation -> {
      @SuppressWarnings("unchecked")
      final var call = (ClientCall<Rs3.WriteWALSegmentRequest, Rs3.WriteWALSegmentResult>)
          invocation.callRealMethod();
      final var callSpy = Mockito.spy(call);
      Mockito.doThrow(new StatusRuntimeException(Status.UNAVAILABLE))
          .when(callSpy).sendMessage(any());
      return callSpy;
    }).doCallRealMethod()
        .when(channel)
        .newCall(eq(RS3Grpc.getWriteWALSegmentStreamMethod()), any());

    final var key = new byte[] { 0 };
    final var value = new byte[] { 1 };
    final var flushedOffset = client.writeWalSegment(
        STORE_ID,
        LSS_ID,
        PSS_ID,
        Optional.empty(),
        5L,
        Collections.singletonList(new Put(key, value))
    );
    assertThat(flushedOffset, is(Optional.of(5L)));
  }

  private void assertNext(
      KeyValueIterator<Bytes, byte[]> iter,
      String key,
      String value
  ) {
    assertThat(iter.hasNext(), is(true));
    final var keyValue = iter.next();
    assertThat(keyValue.key, is(utf8Bytes(key)));
    assertThat(Bytes.wrap(keyValue.value), is(utf8Bytes(value)));
  }

  private Bytes utf8Bytes(String s) {
    final var bytes = s.getBytes(StandardCharsets.UTF_8);
    return Bytes.wrap(bytes);
  }

  private Put buildPut(String key, String value) {
    final var keyBytes = utf8Bytes(key).get();
    final var valueBytes = value == null ? null : utf8Bytes(value).get();
    return new Put(keyBytes, valueBytes);
  }

  static class BasicKeyValueStore implements TestGrpcRs3Service.KeyValueStore {
    private final ConcurrentSkipListMap<Bytes, Bytes> table = new ConcurrentSkipListMap<>();

    @Override
    public void put(final Rs3.KeyValue kvProto) {
      if (!kvProto.hasBasicKv()) {
        throw new UnsupportedOperationException("Unsupported kv type");
      }
      final var kv = kvProto.getBasicKv();
      final var keyBytes = Bytes.wrap(kv.getKey().getKey().toByteArray());
      final var valueBytes = Bytes.wrap(kv.getValue().getValue().toByteArray());
      table.put(keyBytes, valueBytes);
    }

    @Override
    public void delete(final Rs3.Key keyProto) {
      if (!keyProto.hasBasicKey()) {
        throw new UnsupportedOperationException("Unsupported kv type");
      }
      final var keyBytes = Bytes.wrap(keyProto.getBasicKey().getKey().toByteArray());
      table.remove(keyBytes);
    }

    @Override
    public Optional<Rs3.KeyValue> get(final Rs3.Key keyProto) {
      if (!keyProto.hasBasicKey()) {
        throw new UnsupportedOperationException("Unsupported kv type");
      }
      final var keyBytes = Bytes.wrap(keyProto.getBasicKey().getKey().toByteArray());
      final var valueBytes = table.get(keyBytes);
      if (valueBytes == null) {
        return Optional.empty();
      } else {
        return Optional.of(
            Rs3.KeyValue.newBuilder()
                .setBasicKv(GrpcRs3Util.basicKeyValueProto(keyBytes.get(), valueBytes.get()))
                .build()
        );
      }
    }

    @Override
    public Stream<Rs3.KeyValue> range(final Rs3.Range rangeProto) {
      if (!rangeProto.hasBasicRange()) {
        throw new UnsupportedOperationException("Unsupported kv type");
      }

      final var basicRange = rangeProto.getBasicRange();
      final var range = new Range<>(
          decodeBound(basicRange.getFrom()),
          decodeBound(basicRange.getTo())
      );

      return table.entrySet().stream()
          .filter(entry -> range.contains(entry.getKey()))
          .map(entry -> Rs3.KeyValue.newBuilder().setBasicKv(
              GrpcRs3Util.basicKeyValueProto(entry.getKey().get(), entry.getValue().get())
          ).build());
    }

    private RangeBound<Bytes> decodeBound(Rs3.BasicBound bound) {
      if (bound.getType() == Rs3.BoundType.UNBOUNDED) {
        return RangeBound.unbounded();
      } else {
        final var key = Bytes.wrap(bound.getKey().getKey().toByteArray());
        if (bound.getType() == Rs3.BoundType.INCLUSIVE) {
          return RangeBound.inclusive(key);
        } else if (bound.getType() == Rs3.BoundType.EXCLUSIVE) {
          return RangeBound.exclusive(key);
        } else {
          throw new UnsupportedOperationException("Unsupported bound type");
        }
      }
    }
  }



}