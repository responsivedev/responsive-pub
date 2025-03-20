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

import static io.grpc.Status.UNAVAILABLE;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import dev.responsive.kafka.api.config.ResponsiveConfig;
import dev.responsive.kafka.internal.db.rs3.client.CurrentOffsets;
import dev.responsive.kafka.internal.db.rs3.client.RangeIterator;
import dev.responsive.kafka.internal.db.rs3.client.LssId;
import dev.responsive.kafka.internal.db.rs3.client.Put;
import dev.responsive.kafka.internal.db.rs3.client.RS3Client;
import dev.responsive.kafka.internal.db.rs3.client.RS3Exception;
import dev.responsive.kafka.internal.db.rs3.client.RS3TimeoutException;
import dev.responsive.kafka.internal.db.rs3.client.RangeBound;
import dev.responsive.kafka.internal.db.rs3.client.StreamSenderMessageReceiver;
import dev.responsive.kafka.internal.db.rs3.client.WalEntry;
import dev.responsive.rs3.RS3Grpc;
import dev.responsive.rs3.Rs3;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.ExponentialBackoff;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.state.KeyValueIterator;

public class GrpcRS3Client implements RS3Client {
  static final long WAL_OFFSET_NONE = Long.MAX_VALUE;

  private final PssStubsProvider stubs;
  private final Time time;
  private final long retryTimeoutMs;

  @VisibleForTesting
  GrpcRS3Client(final PssStubsProvider stubs, final Time time, final long retryTimeoutMs) {
    this.stubs = Objects.requireNonNull(stubs);
    this.time = Objects.requireNonNull(time);
    this.retryTimeoutMs = retryTimeoutMs;
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
    final Rs3.GetOffsetsResult result = withRetry(
        () -> stub.getOffsets(request),
        () -> "GetOffsets(storeId=" + storeId + ", lssId=" + lssId + ", pssId=" + pssId + ")"
    );
    checkField(result::hasWrittenOffset, "writtenOffset");
    checkField(result::hasFlushedOffset, "flushedOffset");
    return new CurrentOffsets(
        result.getWrittenOffset() == WAL_OFFSET_NONE
            ? Optional.empty() : Optional.of(result.getWrittenOffset()),
        result.getFlushedOffset() == WAL_OFFSET_NONE
            ? Optional.empty() : Optional.of(result.getFlushedOffset())
    );
  }

  private void withRetry(Runnable runner, Supplier<String> opDescription) {
    final Supplier<Void> voidSupplier = () -> {
      runner.run();
      return null;
    };
    withRetry(voidSupplier, opDescription);
  }

  private <T> T withRetry(Supplier<T> supplier, Supplier<String> opDescription) {
    // Using Kafka default backoff settings initially. We can pull them up
    // if there is ever strong reason.
    final var backoff = new ExponentialBackoff(50, 2, 1000, 0.2);
    final var startTimeMs = time.milliseconds();
    final var deadlineMs = startTimeMs + retryTimeoutMs;

    var retries = 0;
    long currentTimeMs;

    do {
      try {
        return supplier.get();
      } catch (final StatusRuntimeException e) {
        if (e.getStatus() != UNAVAILABLE) {
          throw new RS3Exception(e);
        }
      }

      retries += 1;
      currentTimeMs = time.milliseconds();
      time.sleep(Math.min(
          backoff.backoff(retries),
          Math.max(0, deadlineMs - currentTimeMs))
      );
    } while (currentTimeMs - startTimeMs < retryTimeoutMs);
    throw new RS3TimeoutException("Timeout while attempting operation " + opDescription.get());
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
  public KeyValueIterator<Bytes, byte[]> range(
      final UUID storeId,
      final LssId lssId,
      final int pssId,
      final Optional<Long> expectedWrittenOffset,
      RangeBound from,
      RangeBound to
  ) {
    final var requestBuilder = Rs3.RangeRequest.newBuilder()
        .setStoreId(uuidProto(storeId))
        .setLssId(lssIdProto(lssId))
        .setPssId(pssId)
        .setFrom(protoBound(from))
        .setTo(protoBound(to));
    expectedWrittenOffset.ifPresent(requestBuilder::setExpectedWrittenOffset);
    final var asyncStub = stubs.stubs(storeId, pssId).asyncStub();
    final var rangeProxy = new RangeProxy(requestBuilder, asyncStub);
    return new GrpcKeyValueIterator(rangeProxy);
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

    final Rs3.GetResult result = withRetry(
        () -> stub.get(request),
        () -> "Get(storeId=" + storeId + ", lssId=" + lssId + ", pssId=" + pssId + ")"
    );
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

  private Rs3.Bound protoBound(RangeBound bound) {
    return bound.map(new RangeBound.Mapper<>() {
      @Override
      public Rs3.Bound map(final RangeBound.InclusiveBound b) {
        return Rs3.Bound.newBuilder()
            .setType(Rs3.Bound.Type.INCLUSIVE)
            .setKey(ByteString.copyFrom(b.key()))
            .build();
      }

      @Override
      public Rs3.Bound map(final RangeBound.ExclusiveBound b) {
        return Rs3.Bound.newBuilder()
            .setType(Rs3.Bound.Type.EXCLUSIVE)
            .setKey(ByteString.copyFrom(b.key()))
            .build();
      }

      @Override
      public Rs3.Bound map(final RangeBound.Unbounded b) {
        return Rs3.Bound.newBuilder()
            .setType(Rs3.Bound.Type.UNBOUNDED)
            .build();
      }
    });
  }

  private class RangeProxy implements GrpcRangeRequestProxy {
    private final Rs3.RangeRequest.Builder requestBuilder;
    private final RS3Grpc.RS3Stub stub;

    private RangeProxy(
        final Rs3.RangeRequest.Builder requestBuilder,
        final RS3Grpc.RS3Stub stub
    ) {
      this.requestBuilder = requestBuilder;
      this.stub = stub;
    }

    @Override
    public void send(final RangeBound start, final StreamObserver<Rs3.RangeResult> resultObserver) {
      requestBuilder.setFrom(protoBound(start));
      withRetry(
          () -> stub.range(requestBuilder.build(), resultObserver),
          () -> "Range()"
      );
    }
  }

  public static class Connector {
    private final Time time;
    private final String host;
    private final int port;

    private boolean useTls = ResponsiveConfig.RS3_TLS_ENABLED_DEFAULT;
    private long retryTimeoutMs = ResponsiveConfig.RS3_RETRY_TIMEOUT_DEFAULT;

    public Connector(
        final Time time,
        final String host,
        final int port
    ) {
      this.time = Objects.requireNonNull(time);
      this.host = Objects.requireNonNull(host);
      this.port = port;
    }

    public void useTls(boolean useTls) {
      this.useTls = useTls;
    }

    public void retryTimeoutMs(long retryTimeoutMs) {
      this.retryTimeoutMs = retryTimeoutMs;
    }

    public RS3Client connect() {
      String target = String.format("%s:%d", host, port);
      return new GrpcRS3Client(
          PssStubsProvider.connect(target, useTls),
          time,
          retryTimeoutMs
      );
    }
  }

}
