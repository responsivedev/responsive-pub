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

import com.google.protobuf.ByteString;
import dev.responsive.kafka.internal.db.rs3.client.CreateStoreTypes;
import dev.responsive.kafka.internal.db.rs3.client.Delete;
import dev.responsive.kafka.internal.db.rs3.client.LssId;
import dev.responsive.kafka.internal.db.rs3.client.PssCheckpoint;
import dev.responsive.kafka.internal.db.rs3.client.Put;
import dev.responsive.kafka.internal.db.rs3.client.RS3Exception;
import dev.responsive.kafka.internal.db.rs3.client.RS3TransientException;
import dev.responsive.kafka.internal.db.rs3.client.StoreInfo;
import dev.responsive.kafka.internal.utils.WindowedKey;
import dev.responsive.rs3.Rs3;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;

public class GrpcRs3Util {
  public static final Rs3.WALOffset UNWRITTEN_WAL_OFFSET = Rs3.WALOffset.newBuilder()
      .setIsWritten(false)
      .build();

  public static RuntimeException wrapThrowable(Throwable t) {
    final var statusOpt = getGrpcStatus(t);
    if (statusOpt.isPresent()) {
      final var status = statusOpt.get();
      if (isRetriable(status)) {
        return new RS3TransientException(t);
      } else if (status.getCode() == Status.Code.CANCELLED) {
        final var cause = t.getCause();
        final var causeStatus = getGrpcStatus(cause);
        if (causeStatus.map(GrpcRs3Util::isRetriable).orElse(false)) {
          return new RS3TransientException(t);
        }
      }
      return new RS3Exception(t);
    } else if (t instanceof RuntimeException) {
      return (RuntimeException) t;
    } else {
      return new RuntimeException(t);
    }
  }

  private static boolean isRetriable(Status status) {
    return status.getCode() == Status.Code.UNAVAILABLE
        || status.getCode() == Status.Code.RESOURCE_EXHAUSTED;
  }

  private static Optional<Status> getGrpcStatus(Throwable t) {
    if (t instanceof StatusException) {
      return Optional.of(((StatusException) t).getStatus());
    } else if (t instanceof StatusRuntimeException) {
      return Optional.of(((StatusRuntimeException) t).getStatus());
    } else {
      return Optional.empty();
    }
  }

  public static Rs3.BasicKey basicKeyProto(final byte[] key) {
    return Rs3.BasicKey.newBuilder()
        .setKey(ByteString.copyFrom(key))
        .build();
  }

  public static Rs3.WindowKey windowKeyProto(final WindowedKey key) {
    return Rs3.WindowKey.newBuilder()
        .setKey(ByteString.copyFrom(key.key.get()))
        .setWindowTimestamp(key.windowStartMs)
        .build();
  }

  public static Rs3.BasicKeyValue basicKeyValueProto(
      final byte[] key,
      final byte[] value
  ) {
    final var keyProto = basicKeyProto(key);
    final var valueBldr = Rs3.BasicValue.newBuilder();
    valueBldr.setValue(ByteString.copyFrom(value));

    return Rs3.BasicKeyValue.newBuilder()
        .setKey(keyProto)
        .setValue(valueBldr)
        .build();
  }

  public static Rs3.WindowKeyValue windowKeyValueProto(
      final WindowedKey key,
      final byte[] value
  ) {
    final var keyProto = windowKeyProto(key);
    final var valueBldr = Rs3.WindowValue.newBuilder();
    valueBldr.setValue(ByteString.copyFrom(value));
    return Rs3.WindowKeyValue.newBuilder()
        .setKey(keyProto)
        .setValue(valueBldr)
        .build();
  }

  public static Rs3.WriteWALSegmentRequest.Put basicPutProto(final Put put) {
    final var kv = basicKeyValueProto(put.key(), put.value());
    return Rs3.WriteWALSegmentRequest.Put.newBuilder()
        .setKv(Rs3.KeyValue.newBuilder().setBasicKv(kv).build())
        .setTtl(Rs3.Ttl.newBuilder().setTtlType(Rs3.Ttl.TtlType.DEFAULT))
        .build();
  }

  public static Rs3.WriteWALSegmentRequest.Delete basicDeleteProto(final Delete delete) {
    final var key = basicKeyProto(delete.key());
    return Rs3.WriteWALSegmentRequest.Delete.newBuilder()
        .setKey(Rs3.Key.newBuilder().setBasicKey(key))
        .build();
  }

  public static Rs3.StoreType storeTypeToProto(final CreateStoreTypes.StoreType storeType) {
    switch (storeType) {
      case BASIC:
        return Rs3.StoreType.BASIC;
      case WINDOW:
        return Rs3.StoreType.WINDOW;
      default:
        throw new IllegalArgumentException("Unknown store type: " + storeType);
    }
  }

  public static CreateStoreTypes.StoreType storeTypeFromProto(final Rs3.StoreType storeType) {
    switch (storeType) {
      case BASIC:
        return CreateStoreTypes.StoreType.BASIC;
      case WINDOW:
        return CreateStoreTypes.StoreType.WINDOW;
      default:
        throw new IllegalArgumentException("Unknown store type: " + storeType);
    }
  }

  public static StoreInfo.Status storeStatusFromProto(final Rs3.StoreInfo.Status status) {
    switch (status) {
      case CREATING:
        return StoreInfo.Status.CREATING;
      case READY:
        return StoreInfo.Status.READY;
      case DELETING:
        return StoreInfo.Status.DELETED;
      default:
        throw new IllegalArgumentException("Unknown store status: " + status);
    }
  }

  public static Rs3.CreateStoreOptions createStoreOptionsProto(
      CreateStoreTypes.CreateStoreOptions options
  ) {
    final var builder = Rs3.CreateStoreOptions.newBuilder();
    builder.setLogicalShards(options.logicalShards());
    builder.setStoreType(storeTypeToProto(options.storeType()));
    options.clockType().ifPresent(
        type -> builder.setClockType(Rs3.ClockType.forNumber(type.ordinal()))
    );
    options.defaultTtl().ifPresent(builder::setDefaultTtl);
    options.slateDbOptions().ifPresent(slateDbOptions -> {
      final var storageOptions = Rs3.SlateDbStorageOptions.newBuilder();
      slateDbOptions.filterBitsPerKey().ifPresent(storageOptions::setFilterBitsPerKey);
      builder.setSlatedbStorageOptions(storageOptions);
    });
    return builder.build();
  }

  public static Rs3.WALOffset walOffsetProto(final long offset) {
    return Rs3.WALOffset.newBuilder()
        .setIsWritten(true)
        .setOffset(offset)
        .build();
  }

  public static Rs3.WALOffset walOffsetProto(final Optional<Long> offset) {
    return offset
        .map(GrpcRs3Util::walOffsetProto)
        .orElse(UNWRITTEN_WAL_OFFSET);
  }

  public static Optional<Long> walOffsetFromProto(final Rs3.WALOffset walOffset) {
    if (!walOffset.hasIsWritten()) {
      throw new RS3Exception("illegal wal offset: is_written must be set");
    }
    if (walOffset.getIsWritten()) {
      if (!walOffset.hasOffset()) {
        throw new RS3Exception("illegal wal offset: offset must be set");
      }
      return Optional.of(walOffset.getOffset());
    } else {
      return Optional.empty();
    }
  }

  public static UUID uuidFromProto(final Rs3.UUID uuidProto) {
    checkField(uuidProto::hasHigh, "high");
    checkField(uuidProto::hasLow, "low");
    return new UUID(uuidProto.getHigh(), uuidProto.getLow());
  }

  public static Rs3.UUID uuidToProto(final UUID uuid) {
    return Rs3.UUID.newBuilder()
        .setHigh(uuid.getMostSignificantBits())
        .setLow(uuid.getLeastSignificantBits())
        .build();
  }

  public static PssCheckpoint pssCheckpointFromProto(
      final UUID storeId,
      final int pssId,
      final Rs3.StorageCheckpoint checkpointProto
  ) {
    checkField(checkpointProto::hasSlatedbStorageCheckpoint, "slatedbStorageCheckpoint");
    final var slateDbCheckpointProto = checkpointProto.getSlatedbStorageCheckpoint();
    checkField(slateDbCheckpointProto::hasPath, "path");
    checkField(slateDbCheckpointProto::hasCheckpointId, "checkpointId");
    final var slateDbCheckpoint = new PssCheckpoint.SlateDbStorageCheckpoint(
        slateDbCheckpointProto.getPath(),
        uuidFromProto(slateDbCheckpointProto.getCheckpointId())
    );
    return new PssCheckpoint(storeId, pssId, slateDbCheckpoint, null);
  }

  public static Rs3.LSSId lssIdProto(final LssId lssId) {
    return Rs3.LSSId.newBuilder()
        .setId(lssId.id())
        .build();
  }

  public static void checkField(final Supplier<Boolean> check, final String field) {
    if (!check.get()) {
      throw new RuntimeException("rs3 resp proto missing field " + field);
    }
  }
}
