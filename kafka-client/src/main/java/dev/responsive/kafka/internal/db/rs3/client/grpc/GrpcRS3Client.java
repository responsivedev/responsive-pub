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

import static dev.responsive.kafka.internal.db.rs3.client.grpc.GrpcRs3Util.basicDeleteProto;
import static dev.responsive.kafka.internal.db.rs3.client.grpc.GrpcRs3Util.basicKeyProto;
import static dev.responsive.kafka.internal.db.rs3.client.grpc.GrpcRs3Util.basicPutProto;
import static dev.responsive.kafka.internal.utils.Utils.lssIdProto;
import static dev.responsive.kafka.internal.utils.Utils.uuidFromProto;
import static dev.responsive.kafka.internal.utils.Utils.uuidToProto;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import dev.responsive.kafka.api.config.ResponsiveConfig;
import dev.responsive.kafka.internal.db.rs3.client.CreateStoreTypes.CreateStoreOptions;
import dev.responsive.kafka.internal.db.rs3.client.CreateStoreTypes.CreateStoreResult;
import dev.responsive.kafka.internal.db.rs3.client.CurrentOffsets;
import dev.responsive.kafka.internal.db.rs3.client.Delete;
import dev.responsive.kafka.internal.db.rs3.client.LssId;
import dev.responsive.kafka.internal.db.rs3.client.RS3Client;
import dev.responsive.kafka.internal.db.rs3.client.RS3TimeoutException;
import dev.responsive.kafka.internal.db.rs3.client.RS3TransientException;
import dev.responsive.kafka.internal.db.rs3.client.Range;
import dev.responsive.kafka.internal.db.rs3.client.RangeBound;
import dev.responsive.kafka.internal.db.rs3.client.Store;
import dev.responsive.kafka.internal.db.rs3.client.StreamSenderMessageReceiver;
import dev.responsive.kafka.internal.db.rs3.client.WalEntry;
import dev.responsive.kafka.internal.utils.WindowedKey;
import dev.responsive.rs3.RS3Grpc;
import dev.responsive.rs3.Rs3;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.ExponentialBackoff;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.state.KeyValueIterator;

public class GrpcRS3Client implements RS3Client {
  public static final long WAL_OFFSET_NONE = Long.MAX_VALUE;

  private final PssStubsProvider stubs;
  private final Time time;
  private final long retryTimeoutMs;

  // Using Kafka default backoff settings initially. We can pull them up
  // if there is ever strong reason.
  private final ExponentialBackoff backoff = new ExponentialBackoff(50, 2, 1000, 0.2);

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
        .setStoreId(uuidToProto(storeId))
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

  private void withRetry(Runnable grpcOperation, Supplier<String> opDescription) {
    final Supplier<Void> voidSupplier = () -> {
      grpcOperation.run();
      return null;
    };
    withRetry(voidSupplier, opDescription);
  }

  private <T> T withRetry(Supplier<T> grpcOperation, Supplier<String> opDescription) {
    final var deadlineMs = time.milliseconds() + retryTimeoutMs;
    return withRetryDeadline(grpcOperation, deadlineMs, opDescription);
  }

  private <T> T withRetryDeadline(
      Supplier<T> grpcOperation,
      long deadlineMs,
      Supplier<String> opDescription
  ) {
    var retries = 0;
    long currentTimeMs;

    do {
      try {
        return grpcOperation.get();
      } catch (final Throwable t) {
        var wrappedException = GrpcRs3Util.wrapThrowable(t);
        if (!(wrappedException instanceof RS3TransientException)) {
          throw wrappedException;
        }
      }

      retries += 1;
      currentTimeMs = time.milliseconds();
      time.sleep(Math.min(
          backoff.backoff(retries),
          Math.max(0, deadlineMs - currentTimeMs))
      );
    } while (currentTimeMs < deadlineMs);
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
    final var streamObserver = withRetry(
        () -> asyncStub.writeWALSegmentStream(resultObserver),
        () -> "OpenWriteWalSegmentStream()"
    );
    final var streamSender = new GrpcStreamSender<WalEntry, Rs3.WriteWALSegmentRequest>(
        entry -> {
          final var entryBuilder = Rs3.WriteWALSegmentRequest.newBuilder()
              .setStoreId(uuidToProto(storeId))
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
      final List<WalEntry> entries
  ) {
    return withRetry(
        () -> tryWriteWalSegment(storeId, lssId, pssId, expectedWrittenOffset, endOffset, entries),
        () -> "WriteWalSegment(storeId=" + storeId + ", lssId=" + lssId + ", pssId=" + pssId + ")"
    );
  }

  private Optional<Long> tryWriteWalSegment(
      final UUID storeId,
      final LssId lssId,
      final int pssId,
      final Optional<Long> expectedWrittenOffset,
      final long endOffset,
      final List<WalEntry> entries
  ) {
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
      result = senderReceiver.completion().toCompletableFuture().get();
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
      Range range
  ) {
    return sendRange(
        storeId,
        lssId,
        pssId,
        expectedWrittenOffset,
        range,
        GrpcRangeKeyCodec.STANDARD_CODEC
    );
  }

  @Override
  public KeyValueIterator<WindowedKey, byte[]> windowedRange(
      final UUID storeId,
      final LssId lssId,
      final int pssId,
      final Optional<Long> expectedWrittenOffset,
      final RangeBound<WindowedKey> from,
      final RangeBound<WindowedKey> to
  ) {
    return sendRange(
        storeId,
        lssId,
        pssId,
        expectedWrittenOffset,
        new Range<>(from, to),
        GrpcRangeKeyCodec.WINDOW_CODEC
    );
  }

  private <K> KeyValueIterator<K, byte[]> sendRange(
      final UUID storeId,
      final LssId lssId,
      final int pssId,
      final Optional<Long> expectedWrittenOffset,
      final Range range,
      final GrpcRangeKeyCodec<K> keyCodec
  ) {
    final var requestBuilder = Rs3.RangeRequest.newBuilder()
        .setStoreId(uuidToProto(storeId))
        .setLssId(lssIdProto(lssId))
        .setPssId(pssId);

    expectedWrittenOffset.ifPresent(requestBuilder::setExpectedWrittenOffset);
    final Supplier<String> rangeDescription =
        () -> "Range(storeId=" + storeId + ", lssId=" + lssId + ", pssId=" + pssId + ")";
    final var asyncStub = stubs.stubs(storeId, pssId).asyncStub();
    final var rangeProxy = new RangeProxy<>(
        requestBuilder,
        asyncStub,
        keyCodec,
        rangeDescription
    );
    return new GrpcKeyValueIterator<>(range, rangeProxy, keyCodec);
  }

  @Override
  public Optional<byte[]> get(
      final UUID storeId,
      final LssId lssId,
      final int pssId,
      final Optional<Long> expectedWrittenOffset,
      final Bytes key
  ) {
    final var requestBuilder = Rs3.GetRequest.newBuilder()
        .setKey(ByteString.copyFrom(key.get()));
    return sendGet(
        requestBuilder,
        storeId,
        lssId,
        pssId,
        expectedWrittenOffset
    );
  }

  @Override
  public Optional<byte[]> windowedGet(
      final UUID storeId,
      final LssId lssId,
      final int pssId,
      final Optional<Long> expectedWrittenOffset,
      final WindowedKey key
  ) {
    final var requestBuilder = Rs3.GetRequest.newBuilder()
        .setKey(ByteString.copyFrom(key.key.get()))
        .setWindowTimestamp(key.windowStartMs);
    return sendGet(
        requestBuilder,
        storeId,
        lssId,
        pssId,
        expectedWrittenOffset
    );
  }

  private Optional<byte[]> sendGet(
      final Rs3.GetRequest.Builder requestBuilder,
      final UUID storeId,
      final LssId lssId,
      final int pssId,
      final Optional<Long> expectedWrittenOffset
  ) {
    requestBuilder.setStoreId(uuidToProto(storeId))
        .setLssId(lssIdProto(lssId))
        .setPssId(pssId);
        .setKey(Rs3.Key.newBuilder().setBasicKey(basicKeyProto(key)));
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
    checkField(keyValue::hasBasicKv, "value");
    final var value = keyValue.getBasicKv().getValue().getValue();
    return Optional.of(value.toByteArray());
  }

  @Override
  public List<Store> listStores() {
    final var request = Rs3.ListStoresRequest.newBuilder().build();
    final RS3Grpc.RS3BlockingStub stub = stubs.globalStubs().syncStub();

    final Rs3.ListStoresResult result = withRetry(
        () -> stub.listStores(request),
        () -> "ListStores()"
    );

    return result.getStoresList()
        .stream()
        .map(t -> new Store(t.getStoreName(), uuidFromProto(t.getStoreId()), t.getPssIdsList()))
        .collect(Collectors.toList());
  }

  @Override
  public CreateStoreResult createStore(
      final String storeName,
      final CreateStoreOptions options
  ) {
    final var request = Rs3.CreateStoreRequest.newBuilder()
        .setStoreName(storeName)
        .setOptions(GrpcRs3Util.createStoreOptionsProto(options))
        .build();
    final RS3Grpc.RS3BlockingStub stub = stubs.globalStubs().syncStub();

    final Rs3.CreateStoreResult result = withRetry(
        () -> stub.createStore(request),
        () -> "CreateStore(storeName=" + storeName  + ", createStoreOptions=" + options + ")"
    );

    return new CreateStoreResult(uuidFromProto(result.getStoreId()), result.getPssIdsList());
  }

  private void addWalEntryToSegment(
      final WalEntry entry,
      final Rs3.WriteWALSegmentRequest.Builder builder
  ) {
    final var putBuilder = Rs3.WriteWALSegmentRequest.Put.newBuilder();
    final var writer = new WalEntryPutWriter(putBuilder);
    entry.visit(writer);
    builder.setPut(putBuilder);
  }

  private void checkField(final Supplier<Boolean> check, final String field) {
    if (!check.get()) {
      throw new RuntimeException("rs3 resp proto missing field " + field);
    }
  }

  private Rs3.Range protoRange(Range range) {
    final var protoRange = Rs3.BasicRange.newBuilder()
        .setFrom(protoBound(range.start()))
        .setTo(protoBound(range.end()));
    return Rs3.Range.newBuilder()
        .setBasicRange(protoRange)
        .build();
  }

  private Rs3.BasicBound protoBound(RangeBound bound) {
    return bound.map(new RangeBound.Mapper<>() {
      @Override
      public Rs3.BasicBound map(final RangeBound.InclusiveBound b) {
        return Rs3.BasicBound.newBuilder()
            .setType(Rs3.BoundType.INCLUSIVE)
            .setKey(basicKeyProto(b.key()))
            .build();
      }

      @Override
      public Rs3.BasicBound map(final RangeBound.ExclusiveBound b) {
        return Rs3.BasicBound.newBuilder()
            .setType(Rs3.BoundType.EXCLUSIVE)
            .setKey(basicKeyProto(b.key()))
            .build();
      }

      @Override
      public Rs3.BasicBound map(final RangeBound.Unbounded b) {
        return Rs3.BasicBound.newBuilder()
            .setType(Rs3.BoundType.UNBOUNDED)
            .build();
      }
    });
  }

  private class RangeProxy<K> implements GrpcRangeRequestProxy<K> {
    private final Rs3.RangeRequest.Builder requestBuilder;
    private final RS3Grpc.RS3Stub stub;
    private final Supplier<String> opDescription;
    private final GrpcRangeKeyCodec<K> keyCodec;
    private int attempts = 0;
    private long deadlineMs = Long.MAX_VALUE; // Set upon the first retry

    private RangeProxy(
        final Rs3.RangeRequest.Builder requestBuilder,
        final RS3Grpc.RS3Stub stub,
        final GrpcRangeKeyCodec<K> keyCodec,
        final Supplier<String> opDescription
    ) {
      this.requestBuilder = requestBuilder;
      this.stub = stub;
      this.keyCodec = keyCodec;
      this.opDescription = opDescription;
    }

    @Override
    public void send(
        final Range<K> range,
        final StreamObserver<Rs3.RangeResult> resultObserver
    ) {
      requestBuilder.setRange(protoRange(range));

      while (true) {
        try {
          final var currentTimeMs = time.milliseconds();
          if (currentTimeMs >= deadlineMs) {
            throw new RS3TimeoutException("Timeout while attempting operation "
                                              + opDescription.get());
          }

          if (attempts > 0) {
            deadlineMs = Math.min(deadlineMs, currentTimeMs + retryTimeoutMs);
            time.sleep(Math.min(
                backoff.backoff(attempts),
                Math.max(0, deadlineMs - currentTimeMs))
            );
          }

          attempts += 1;
          stub.range(requestBuilder.build(), resultObserver);
          return;
        } catch (final Throwable t) {
          var wrappedException = GrpcRs3Util.wrapThrowable(t);
          if (!(wrappedException instanceof RS3TransientException)) {
            throw wrappedException;
          }
        }
      }
    }
  }

  public static class Connector {
    private final Time time;
    private final String host;
    private final int port;
    private final Supplier<ApiCredential> credentialSupplier;

    private boolean useTls = ResponsiveConfig.RS3_TLS_ENABLED_DEFAULT;
    private long retryTimeoutMs = ResponsiveConfig.RS3_RETRY_TIMEOUT_DEFAULT;

    public Connector(
        final Time time,
        final String host,
        final int port,
        final Supplier<ApiCredential> credentialSupplier
    ) {
      this.time = Objects.requireNonNull(time);
      this.host = Objects.requireNonNull(host);
      this.port = port;
      this.credentialSupplier = Objects.requireNonNull(credentialSupplier);
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
          PssStubsProvider.connect(target, useTls, credentialSupplier.get()),
          time,
          retryTimeoutMs
      );
    }
  }

}
