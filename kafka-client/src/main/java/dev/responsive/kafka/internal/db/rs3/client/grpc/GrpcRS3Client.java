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

package dev.responsive.kafka.internal.db.rs3.client.grpc;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import dev.responsive.kafka.api.config.ResponsiveConfig;
import dev.responsive.kafka.internal.db.rs3.client.CurrentOffsets;
import dev.responsive.kafka.internal.db.rs3.client.LssId;
import dev.responsive.kafka.internal.db.rs3.client.Put;
import dev.responsive.kafka.internal.db.rs3.client.RS3Client;
import dev.responsive.kafka.internal.db.rs3.client.StreamSenderMessageReceiver;
import dev.responsive.kafka.internal.db.rs3.client.WalEntry;
import dev.responsive.rs3.RS3Grpc;
import dev.responsive.rs3.Rs3;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

public class GrpcRS3Client implements RS3Client {
  static final long WAL_OFFSET_NONE = Long.MAX_VALUE;

  private final PssStubsProvider stubs;

  @VisibleForTesting
  GrpcRS3Client(final PssStubsProvider stubs) {
    this.stubs = Objects.requireNonNull(stubs);
  }

  public void close() {
    stubs.close();
  }

  @Override
  public CurrentOffsets getCurrentOffsets(final UUID storeId, final LssId lssId, final int pssId) {
    final RS3Grpc.RS3BlockingStub stub = stubs.stubs(storeId, pssId).syncStub();

    final Rs3.GetOffsetsRequest request = Rs3.GetOffsetsRequest.newBuilder()
        .setStoreId(uuidProto(storeId))
        .setLssId(lssIdProto(lssId))
        .setPssId(pssId)
        .build();
    final var result = stub.getOffsets(request);
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
    final RS3Grpc.RS3Stub asyncStub = stubs.stubs(storeId, pssId).asyncStub();
    final var streamObserver = asyncStub.writeWALSegmentStream(resultObserver);
    final var streamSender = new GrpcStreamSender<WalEntry, Rs3.WriteWALSegmentRequest>(
        entry -> {
          final var entryBuilder = Rs3.WriteWALSegmentRequest.newBuilder()
              .setStoreId(uuidProto(storeId))
              .setLssId(lssIdProto(lssId))
              .setPssId(pssId)
              .setEndOffset(endOffset)
              .setExpectedWrittenOffset(expectedWrittenOffset.orElse(WAL_OFFSET_NONE));
          addWalEntryToSegment(entry, entryBuilder);
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
    final var requestBuilder = Rs3.GetRequest.newBuilder()
        .setStoreId(uuidProto(storeId))
        .setLssId(lssIdProto(lssId))
        .setPssId(pssId)
        .setKey(ByteString.copyFrom(key));
    expectedWrittenOffset.ifPresent(requestBuilder::setExpectedWrittenOffset);
    final var request = requestBuilder.build();
    final RS3Grpc.RS3BlockingStub stub = stubs.stubs(storeId, pssId).syncStub();

    final var result = stub.get(request);
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

  private void addWalEntryToSegment(
      final WalEntry entry,
      final Rs3.WriteWALSegmentRequest.Builder builder
  ) {
    if (entry instanceof Put) {
      final var put = (Put) entry;
      final var putBuilder = Rs3.WriteWALSegmentRequest.Put.newBuilder()
          .setKey(ByteString.copyFrom(put.key()));
      if (put.value().isPresent()) {
        putBuilder.setValue(ByteString.copyFrom(put.value().get()));
      }
      putBuilder.setTtl(Rs3.Ttl.newBuilder().setTtlType(Rs3.Ttl.TtlType.DEFAULT).build());
      builder.setPut(putBuilder.build());
    }
  }

  private void checkField(final Supplier<Boolean> check, final String field) {
    if (!check.get()) {
      throw new RuntimeException("rs3 resp proto missing field " + field);
    }
  }

  public static class Connector {
    private final String host;
    private final int port;
    private boolean useTls = ResponsiveConfig.RS3_TLS_ENABLED_DEFAULT;

    public Connector(
        final String host,
        final int port
    ) {
      this.host = Objects.requireNonNull(host);
      this.port = port;
    }

    public void useTls(boolean useTls) {
      this.useTls = useTls;
    }

    public RS3Client connect() {
      String target = String.format("%s:%d", host, port);
      return new GrpcRS3Client(PssStubsProvider.connect(target, useTls));
    }
  }

}
