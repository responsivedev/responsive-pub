package dev.responsive.kafka.internal.db.rs3.client.grpc;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

import com.google.protobuf.ByteString;
import dev.responsive.kafka.internal.db.rs3.client.LssId;
import dev.responsive.kafka.internal.db.rs3.client.Put;
import dev.responsive.kafka.internal.db.rs3.client.Range;
import dev.responsive.kafka.internal.db.rs3.client.RangeBound;
import dev.responsive.rs3.RS3Grpc;
import dev.responsive.rs3.Rs3;
import io.grpc.ClientCall;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class GrpcRS3ClientEndToEndTest {

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
    this.server = InProcessServerBuilder
        .forName(SERVER_NAME)
        .addService(new TestRs3Service())
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
        key
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
        new Range(
            RangeBound.unbounded(),
            RangeBound.unbounded()
        )
    );

    assertNext(iter, "a", "foo");
    assertNext(iter, "b", "bar");
    assertNext(iter, "c", "baz");
    assertThat(iter.hasNext(), is(false));
  }

  private void writeWalSegment(long endOffset, List<Put> puts) {
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
        new Range(
            RangeBound.inclusive("b".getBytes(StandardCharsets.UTF_8)),
            RangeBound.exclusive("e".getBytes(StandardCharsets.UTF_8))
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
        new Range(
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

  static class TestRs3Service extends RS3Grpc.RS3ImplBase {
    private final AtomicLong offset = new AtomicLong(0);
    private final ConcurrentSkipListMap<Bytes, Bytes> table = new ConcurrentSkipListMap<>();

    @Override
    public void getOffsets(
        final Rs3.GetOffsetsRequest req,
        final StreamObserver<Rs3.GetOffsetsResult> responseObserver
    ) {
      final var storeId = new UUID(
          req.getStoreId().getHigh(),
          req.getStoreId().getLow()
      );
      if (req.getPssId() != PSS_ID
          || req.getLssId().getId() != LSS_ID.id()
          || !storeId.equals(STORE_ID)) {
        responseObserver.onError(new StatusRuntimeException(Status.INVALID_ARGUMENT));
      }

      final var currentOffset = offset.get();
      final var result = Rs3.GetOffsetsResult
          .newBuilder()
          .setFlushedOffset(currentOffset)
          .setWrittenOffset(currentOffset)
          .build();
      responseObserver.onNext(result);
      responseObserver.onCompleted();
    }

    @Override
    public void get(
        final Rs3.GetRequest req,
        final StreamObserver<Rs3.GetResult> responseObserver
    ) {
      final var storeId = new UUID(
          req.getStoreId().getHigh(),
          req.getStoreId().getLow()
      );
      if (req.getPssId() != PSS_ID
          || req.getLssId().getId() != LSS_ID.id()
          || !storeId.equals(STORE_ID)) {
        responseObserver.onError(new StatusRuntimeException(Status.INVALID_ARGUMENT));
        return;
      }

      if (req.getExpectedWrittenOffset() != GrpcRS3Client.WAL_OFFSET_NONE) {
        if (offset.get() < req.getExpectedWrittenOffset()) {
          responseObserver.onError(new StatusRuntimeException(Status.INVALID_ARGUMENT));
          return;
        }
      }

      final var key = req.getKey().getBasicKey();
      final var kvBuilder = Rs3.BasicKeyValue.newBuilder().setKey(key);

      final var keyBytes = Bytes.wrap(key.getKey().toByteArray());
      final var valueBytes = table.get(keyBytes);
      if (valueBytes != null) {
        final var value = Rs3.BasicValue.newBuilder()
            .setValue(ByteString.copyFrom(valueBytes.get()));
        kvBuilder.setValue(value);
      }

      final var result = Rs3.GetResult
          .newBuilder()
          .setResult(Rs3.KeyValue.newBuilder().setBasicKv(kvBuilder))
          .build();
      responseObserver.onNext(result);
      responseObserver.onCompleted();
    }

    @Override
    public void range(
        final Rs3.RangeRequest req,
        final StreamObserver<Rs3.RangeResult> responseObserver
    ) {
      final var storeId = new UUID(
          req.getStoreId().getHigh(),
          req.getStoreId().getLow()
      );
      if (req.getPssId() != PSS_ID
          || req.getLssId().getId() != LSS_ID.id()
          || !storeId.equals(STORE_ID)) {
        responseObserver.onError(new StatusRuntimeException(Status.INVALID_ARGUMENT));
        return;
      }

      if (req.getExpectedWrittenOffset() != GrpcRS3Client.WAL_OFFSET_NONE) {
        if (offset.get() < req.getExpectedWrittenOffset()) {
          responseObserver.onError(new StatusRuntimeException(Status.INVALID_ARGUMENT));
          return;
        }
      }

      final var range = GrpsRs3TestUtil.newRangeFromProto(req);
      for (final var keyValueEntry : table.entrySet()) {
        if (!range.contains(keyValueEntry.getKey().get())) {
          continue;
        }

        final var keyValue = GrpcRs3Util.basicKeyValueProto(
            keyValueEntry.getKey().get(),
            keyValueEntry.getValue().get()
        );

        final var keyValueResult = Rs3.RangeResult.newBuilder()
            .setType(Rs3.RangeResult.Type.RESULT)
            .setResult(Rs3.KeyValue.newBuilder().setBasicKv(keyValue))
            .build();

        responseObserver.onNext(keyValueResult);
      }

      final var endOfStream = Rs3.RangeResult.newBuilder()
          .setType(Rs3.RangeResult.Type.END_OF_STREAM)
          .build();
      responseObserver.onNext(endOfStream);
      responseObserver.onCompleted();
    }

    @Override
    public StreamObserver<Rs3.WriteWALSegmentRequest> writeWALSegmentStream(
        final StreamObserver<Rs3.WriteWALSegmentResult> responseObserver
    ) {
      return new StreamObserver<>() {
        @Override
        public void onNext(final Rs3.WriteWALSegmentRequest req) {
          final var storeId = new UUID(
              req.getStoreId().getHigh(),
              req.getStoreId().getLow()
          );
          if (req.getPssId() != PSS_ID
              || req.getLssId().getId() != LSS_ID.id()
              || !storeId.equals(STORE_ID)) {
            responseObserver.onError(new StatusRuntimeException(Status.INVALID_ARGUMENT));
          }

          if (req.getExpectedWrittenOffset() != GrpcRS3Client.WAL_OFFSET_NONE) {
            if (offset.get() < req.getExpectedWrittenOffset()) {
              responseObserver.onError(new StatusRuntimeException(Status.INVALID_ARGUMENT));
              return;
            }
          }

          TestRs3Service.this.offset.getAndUpdate(
              current -> Math.max(current, req.getEndOffset())
          );
          if (req.hasPut()) {
            final var kv = req.getPut().getKv().getBasicKv();
            final var keyBytes = Bytes.wrap(kv.getKey().getKey().toByteArray());
            final var valueBytes = Bytes.wrap(kv.getValue().getValue().toByteArray());
            table.put(keyBytes, valueBytes);
          } else if (req.hasDelete()) {
            final var key = req.getDelete().getKey().getBasicKey();
            final var keyBytes = Bytes.wrap(key.getKey().toByteArray());
            table.remove(keyBytes);
          }
        }

        @Override
        public void onError(final Throwable throwable) {
          responseObserver.onError(throwable);
        }

        @Override
        public void onCompleted() {
          final var result = Rs3.WriteWALSegmentResult
              .newBuilder()
              .setFlushedOffset(offset.get())
              .build();
          responseObserver.onNext(result);
          responseObserver.onCompleted();
        }
      };
    }
  }

}