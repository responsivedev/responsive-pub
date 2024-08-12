package dev.responsive.kafka.internal.db.pocket.client.grpc;

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
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import io.grpc.TlsChannelCredentials;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

public class GrpcPocketClient implements PocketClient {
  private final ManagedChannel channel;
  private final OtterPocketGrpc.OtterPocketBlockingStub stub;
  private final OtterPocketGrpc.OtterPocketStub asyncStub;

  GrpcPocketClient(
      final ManagedChannel channel,
      final OtterPocketGrpc.OtterPocketBlockingStub stub,
      final OtterPocketGrpc.OtterPocketStub asyncStub
  ) {
    this.channel = Objects.requireNonNull(channel);
    this.stub = Objects.requireNonNull(stub);
    this.asyncStub = Objects.requireNonNull(asyncStub);
  }

  public static GrpcPocketClient connect(
      final String target
  ) {
    final ChannelCredentials channelCredentials = TlsChannelCredentials.create();
    final ManagedChannel channel = Grpc.newChannelBuilder(target, channelCredentials).build();
    final OtterPocketGrpc.OtterPocketBlockingStub stub = OtterPocketGrpc.newBlockingStub(channel);
    final OtterPocketGrpc.OtterPocketStub asyncStub = OtterPocketGrpc.newStub(channel);
    return new GrpcPocketClient(channel, stub, asyncStub);
  }

  @Override
  public CurrentOffsets getCurrentOffsets(final LssId lssId, final int pssId) {
    final Otterpocket.GetOffsetsResult result;
    try {
      result = stub.getOffsets(Otterpocket.GetOffsetsRequest.newBuilder()
          .setLssId(lssIdProto(lssId))
          .setPssId(pssId)
          .build());
    } catch (final StatusRuntimeException e) {
      throw new PocketException(e);
    }
    checkField(result::hasFlushedOffset, "flushedOffset");
    checkField(result::hasWrittenOffset, "writtenOffset");
    return new CurrentOffsets(
        result.getWrittenOffset(),
        result.getFlushedOffset()
    );
  }

  @Override
  public StreamSenderMessageReceiver<WalEntry, Long> writeWalSegmentAsync(
      final LssId lssId,
      final int pssId,
      final long expectedWrittenOffset,
      final long endOffset
  ) {
    final GrpcMessageReceiver<Otterpocket.WriteWALSegmentResult> resultObserver = new GrpcMessageReceiver<>();
    final var streamObserver = asyncStub.writeWALSegmentStream(resultObserver);
    final var streamSender = new GrpcStreamSender<WalEntry, Otterpocket.WriteWALSegmentRequest>(
        entry -> {
          final var entryBuilder = Otterpocket.WriteWALSegmentRequest.newBuilder()
              .setLssId(lssIdProto(lssId))
              .setPssId(pssId)
              .setExpectedWrittenOffset(expectedWrittenOffset)
              .setEndOffset(endOffset);
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
            .thenApply(Otterpocket.WriteWALSegmentResult::getFlushedOffset)
    );
  }

  @Override
  public long writeWalSegment(
      final LssId lssId,
      final int pssId,
      final long expectedWrittenOffset,
      final long endOffset,
      final List<WalEntry> entries) {
    final var senderReceiver = writeWalSegmentAsync(lssId, pssId, expectedWrittenOffset, endOffset);
    for (final WalEntry entry : entries) {
      senderReceiver.sender().sendNext(entry);
    }
    senderReceiver.sender().finish();
    final long result;
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
  public Optional<byte[]> get(LssId lssId, int pssId, Optional<Long> expectedWrittenOffset, byte[] key) {
    final Otterpocket.GetResult result;
    try {
      result = stub.get(Otterpocket.GetRequest.newBuilder()
          .setLssId(lssIdProto(lssId))
          .setPssId(pssId)
          .setKey(ByteString.copyFrom(key))
          .build());
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

  private Otterpocket.LSSId lssIdProto(final LssId lssId) {
    return Otterpocket.LSSId.newBuilder()
        .setId(lssId.id())
        .build();
  }

  private void checkField(final Supplier<Boolean> check , final String field) {
    if (check.get()) {
      throw new RuntimeException("otterpocket resp proto missing field " + field);
    }
  }
}
