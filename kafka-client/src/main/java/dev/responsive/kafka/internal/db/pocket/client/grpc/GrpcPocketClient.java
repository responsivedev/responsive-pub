package dev.responsive.kafka.internal.db.pocket.client.grpc;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import dev.responsive.kafka.internal.db.pocket.client.CurrentOffsets;
import dev.responsive.kafka.internal.db.pocket.client.LssId;
import dev.responsive.kafka.internal.db.pocket.client.PocketClient;
import dev.responsive.kafka.internal.db.pocket.client.PocketException;
import dev.responsive.kafka.internal.db.pocket.client.Put;
import dev.responsive.kafka.internal.db.pocket.client.StreamSenderMessageReceiver;
import dev.responsive.kafka.internal.db.pocket.client.WalEntry;
import dev.responsive.otterpocket.OtterPocketGrpc;
import dev.responsive.otterpocket.Otterpocket;
import io.grpc.ChannelCredentials;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

public class GrpcPocketClient implements PocketClient {
  static final long WAL_OFFSET_NONE = Long.MAX_VALUE;

  private final ManagedChannel channel;
  private final OtterPocketGrpc.OtterPocketBlockingStub stub;
  private final OtterPocketGrpc.OtterPocketStub asyncStub;

  @VisibleForTesting
  GrpcPocketClient(
      final ManagedChannel channel,
      final OtterPocketGrpc.OtterPocketBlockingStub stub,
      final OtterPocketGrpc.OtterPocketStub asyncStub
  ) {
    this.channel = Objects.requireNonNull(channel);
    this.stub = Objects.requireNonNull(stub);
    this.asyncStub = Objects.requireNonNull(asyncStub);
  }

  public void close() {
    channel.shutdownNow();
  }

  public static GrpcPocketClient connect(
      final String target
  ) {
    final ChannelCredentials channelCredentials = InsecureChannelCredentials.create();
    final ManagedChannel channel = Grpc.newChannelBuilder(target, channelCredentials).build();
    final OtterPocketGrpc.OtterPocketBlockingStub stub = OtterPocketGrpc.newBlockingStub(channel);
    final OtterPocketGrpc.OtterPocketStub asyncStub = OtterPocketGrpc.newStub(channel);
    return new GrpcPocketClient(channel, stub, asyncStub);
  }

  @Override
  public CurrentOffsets getCurrentOffsets(final UUID storeId, final LssId lssId, final int pssId) {
    final Otterpocket.GetOffsetsResult result;
    try {
      result = stub.getOffsets(Otterpocket.GetOffsetsRequest.newBuilder()
          .setStoreId(uuidProto(storeId))
          .setLssId(lssIdProto(lssId))
          .setPssId(pssId)
          .build());
    } catch (final StatusRuntimeException e) {
      throw new PocketException(e);
    }
    checkField(result::hasWrittenOffset, "writtenOffset");
    checkField(result::hasFlushedOffset, "flushedOffset");
    return new CurrentOffsets(
        result.getWrittenOffset() == WAL_OFFSET_NONE ?
            Optional.empty() : Optional.of(result.getWrittenOffset()),
        result.getFlushedOffset() == WAL_OFFSET_NONE ?
            Optional.empty() : Optional.of(result.getFlushedOffset())
    );
  }

  @Override
  public StreamSenderMessageReceiver<WalEntry, Optional<Long>> writeWalSegmentAsync(
      final UUID storeId,
      final LssId lssId,
      final int pssId,
      final Optional<Long> expectedWrittenOffset,
      final long endOffset
  ) {
    final GrpcMessageReceiver<Otterpocket.WriteWALSegmentResult> resultObserver = new GrpcMessageReceiver<>();
    final var streamObserver = asyncStub.writeWALSegmentStream(resultObserver);
    final var streamSender = new GrpcStreamSender<WalEntry, Otterpocket.WriteWALSegmentRequest>(
        entry -> {
          final var entryBuilder = Otterpocket.WriteWALSegmentRequest.newBuilder()
              .setStoreId(uuidProto(storeId))
              .setLssId(lssIdProto(lssId))
              .setPssId(pssId)
              .setEndOffset(endOffset)
              .setExpectedWrittenOffset(expectedWrittenOffset.orElse(WAL_OFFSET_NONE));
          if (entry instanceof Put) {
            final var put = (Put) entry;
            final var putBuilder = Otterpocket.WriteWALSegmentRequest.Put.newBuilder()
                .setKey(ByteString.copyFrom(put.key()));
            if (put.value().isPresent()) {
              putBuilder.setValue(ByteString.copyFrom(put.value().get()));
            }
            entryBuilder.setPut(putBuilder.build());
          }
          return entryBuilder.build();
        },
        streamObserver
    );
    return new StreamSenderMessageReceiver<>(
        streamSender,
        resultObserver.message()
            .thenApply(r -> {
              checkField(r::hasFlushedOffset, "flushedOffset");
              return r.getFlushedOffset() == WAL_OFFSET_NONE
                  ? Optional.empty() : Optional.of(r.getFlushedOffset());
            })
    );
  }

  @Override
  public Optional<Long> writeWalSegment(
      final UUID storeId,
      final LssId lssId,
      final int pssId,
      final Optional<Long> expectedWrittenOffset,
      final long endOffset,
      final List<WalEntry> entries) {
    final var senderReceiver = writeWalSegmentAsync(
        storeId,
        lssId,
        pssId,
        expectedWrittenOffset,
        endOffset
    );
    for (final WalEntry entry : entries) {
      senderReceiver.sender().sendNext(entry);
    }
    senderReceiver.sender().finish();
    final Optional<Long> result;
    try {
      result = senderReceiver.receiver().toCompletableFuture().get();
    } catch (final ExecutionException e) {
      if (e.getCause() instanceof RuntimeException) {
        throw (RuntimeException) e.getCause();
      }
      throw new RuntimeException(e.getCause());
    } catch (final InterruptedException e) {
      throw new RuntimeException(e);
    }
    return result;
  }

  @Override
  public Optional<byte[]> get(final UUID storeId, LssId lssId, int pssId, Optional<Long> expectedWrittenOffset, byte[] key) {
    final var requestBuilder = Otterpocket.GetRequest.newBuilder()
        .setStoreId(uuidProto(storeId))
        .setLssId(lssIdProto(lssId))
        .setPssId(pssId)
        .setKey(ByteString.copyFrom(key));
    expectedWrittenOffset.ifPresent(requestBuilder::setExpectedWrittenOffset);
    final var request = requestBuilder.build();
    final Otterpocket.GetResult result;
    try {
      result = stub.get(request);
    } catch (final StatusRuntimeException e) {
      throw new PocketException(e);
    }
    if (!result.hasResult()) {
      return Optional.empty();
    }
    final Otterpocket.KeyValue keyValue = result.getResult();
    checkField(keyValue::hasValue, "value");
    return Optional.of(keyValue.getValue().toByteArray());
  }

  private Otterpocket.UUID uuidProto(final UUID uuid) {
    return Otterpocket.UUID.newBuilder()
        .setHigh(uuid.getMostSignificantBits())
        .setLow(uuid.getLeastSignificantBits())
        .build();
  }

  private Otterpocket.LSSId lssIdProto(final LssId lssId) {
    return Otterpocket.LSSId.newBuilder()
        .setId(lssId.id())
        .build();
  }

  private void checkField(final Supplier<Boolean> check , final String field) {
    if (!check.get()) {
      throw new RuntimeException("otterpocket resp proto missing field " + field);
    }
  }
}
