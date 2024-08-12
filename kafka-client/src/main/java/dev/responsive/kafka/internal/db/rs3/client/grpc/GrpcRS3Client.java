package dev.responsive.kafka.internal.db.rs3.client.grpc;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import dev.responsive.kafka.internal.db.rs3.client.CurrentOffsets;
import dev.responsive.kafka.internal.db.rs3.client.LssId;
import dev.responsive.kafka.internal.db.rs3.client.Put;
import dev.responsive.kafka.internal.db.rs3.client.RS3Client;
import dev.responsive.kafka.internal.db.rs3.client.RS3Exception;
import dev.responsive.kafka.internal.db.rs3.client.StreamSenderMessageReceiver;
import dev.responsive.kafka.internal.db.rs3.client.WalEntry;
import dev.responsive.rs3.RS3Grpc;
import dev.responsive.rs3.Rs3;
import io.grpc.ChannelCredentials;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GrpcRS3Client implements RS3Client {
  static final Logger LOG = LoggerFactory.getLogger(GrpcRS3Client.class);

  static final long WAL_OFFSET_NONE = Long.MAX_VALUE;

  private final ManagedChannel channel;
  private final RS3Grpc.RS3BlockingStub stub;
  private final RS3Grpc.RS3Stub asyncStub;
  private final Stats stats = new Stats();

  @VisibleForTesting
  GrpcRS3Client(
      final ManagedChannel channel,
      final RS3Grpc.RS3BlockingStub stub,
      final RS3Grpc.RS3Stub asyncStub
  ) {
    this.channel = Objects.requireNonNull(channel);
    this.stub = Objects.requireNonNull(stub);
    this.asyncStub = Objects.requireNonNull(asyncStub);
  }

  public void close() {
    channel.shutdownNow();
  }

  public static GrpcRS3Client connect(
      final String target
  ) {
    final ChannelCredentials channelCredentials = InsecureChannelCredentials.create();
    final ManagedChannel channel = Grpc.newChannelBuilder(target, channelCredentials)
        .build();
    final RS3Grpc.RS3BlockingStub stub = RS3Grpc.newBlockingStub(channel);
    final RS3Grpc.RS3Stub asyncStub = RS3Grpc.newStub(channel);
    return new GrpcRS3Client(channel, stub, asyncStub);
  }

  @Override
  public CurrentOffsets getCurrentOffsets(final UUID storeId, final LssId lssId, final int pssId) {
    final Rs3.GetOffsetsResult result;
    try {
      result = stub.getOffsets(Rs3.GetOffsetsRequest.newBuilder()
          .setStoreId(uuidProto(storeId))
          .setLssId(lssIdProto(lssId))
          .setPssId(pssId)
          .build());
    } catch (final StatusRuntimeException e) {
      throw new RS3Exception(e);
    }
    checkField(result::hasWrittenOffset, "writtenOffset");
    checkField(result::hasFlushedOffset, "flushedOffset");
    return new CurrentOffsets(
        result.getWrittenOffset() == WAL_OFFSET_NONE
            ? Optional.empty() : Optional.of(result.getWrittenOffset()),
        result.getFlushedOffset() == WAL_OFFSET_NONE
            ? Optional.empty() : Optional.of(result.getFlushedOffset())
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
    final GrpcMessageReceiver<Rs3.WriteWALSegmentResult> resultObserver
        = new GrpcMessageReceiver<>();
    final var streamObserver = asyncStub.writeWALSegmentStream(resultObserver);
    final var streamSender = new GrpcStreamSender<WalEntry, Rs3.WriteWALSegmentRequest>(
        entry -> {
          final var entryBuilder = Rs3.WriteWALSegmentRequest.newBuilder()
              .setStoreId(uuidProto(storeId))
              .setLssId(lssIdProto(lssId))
              .setPssId(pssId)
              .setEndOffset(endOffset)
              .setExpectedWrittenOffset(expectedWrittenOffset.orElse(WAL_OFFSET_NONE));
          if (entry instanceof Put) {
            final var put = (Put) entry;
            final var putBuilder = Rs3.WriteWALSegmentRequest.Put.newBuilder()
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
  public Optional<byte[]> get(
      final UUID storeId,
      final LssId lssId,
      final int pssId,
      final Optional<Long> expectedWrittenOffset,
      final byte[] key
  ) {
    final Instant start = Instant.now();
    final var requestBuilder = Rs3.GetRequest.newBuilder()
        .setStoreId(uuidProto(storeId))
        .setLssId(lssIdProto(lssId))
        .setPssId(pssId)
        .setKey(ByteString.copyFrom(key));
    expectedWrittenOffset.ifPresent(requestBuilder::setExpectedWrittenOffset);
    final var request = requestBuilder.build();
    final Rs3.GetResult result;
    try {
      result = stub.get(request);
    } catch (final StatusRuntimeException e) {
      throw new RS3Exception(e);
    }
    stats.recordRead(Duration.between(start, Instant.now()));
    stats.log();
    if (!result.hasResult()) {
      return Optional.empty();
    }
    final Rs3.KeyValue keyValue = result.getResult();
    checkField(keyValue::hasValue, "value");
    return Optional.of(keyValue.getValue().toByteArray());
  }

  private Rs3.UUID uuidProto(final UUID uuid) {
    return Rs3.UUID.newBuilder()
        .setHigh(uuid.getMostSignificantBits())
        .setLow(uuid.getLeastSignificantBits())
        .build();
  }

  private Rs3.LSSId lssIdProto(final LssId lssId) {
    return Rs3.LSSId.newBuilder()
        .setId(lssId.id())
        .build();
  }

  private void checkField(final Supplier<Boolean> check, final String field) {
    if (!check.get()) {
      throw new RuntimeException("rs3 resp proto missing field " + field);
    }
  }

  private static class Stats {
    private long totalReads = 0;
    private long totalReadsElapsedUs = 0;
    private Instant lastLog = Instant.EPOCH;

    public synchronized void recordRead(final Duration elapsed) {
      totalReads += 1;
      totalReadsElapsedUs += elapsed.toNanos() / 1_000;
    }

    public synchronized void log() {
      final var now = Instant.now();
      if (now.isBefore(lastLog.plus(Duration.ofSeconds(10)))) {
        return;
      }
      lastLog = now;
      LOG.info("rs3 client read statistics: {} {}", totalReads, totalReadsElapsedUs);
    }
  }
}
