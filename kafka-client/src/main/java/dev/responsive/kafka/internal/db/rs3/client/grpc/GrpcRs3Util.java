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
import dev.responsive.kafka.internal.db.rs3.client.Put;
import dev.responsive.kafka.internal.db.rs3.client.RS3Exception;
import dev.responsive.kafka.internal.db.rs3.client.RS3TransientException;
import dev.responsive.kafka.internal.utils.WindowedKey;
import dev.responsive.rs3.Rs3;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import java.util.Optional;

public class GrpcRs3Util {

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

  public static Rs3.StoreType storeTypeProto(final CreateStoreTypes.StoreType storeType) {
    switch (storeType) {
      case BASIC:
        return Rs3.StoreType.BASIC;
      case WINDOW:
        return Rs3.StoreType.WINDOW;
      default:
        throw new IllegalArgumentException("Unknown store type: " + storeType);
    }
  }

  public static Rs3.CreateStoreOptions createStoreOptionsProto(
      CreateStoreTypes.CreateStoreOptions options
  ) {
    final var builder = Rs3.CreateStoreOptions.newBuilder();
    builder.setLogicalShards(options.logicalShards());
    builder.setStoreType(storeTypeProto(options.storeType()));
    options.clockType().ifPresent(
        type -> builder.setClockType(Rs3.CreateStoreOptions.ClockType.forNumber(type.ordinal()))
    );
    options.defaultTtl().ifPresent(builder::setDefaultTtl);
    options.slateDbOptions().ifPresent(slateDbOptions -> {
      final var storageOptions = Rs3.SlateDbStorageOptions.newBuilder();
      slateDbOptions.filterBitsPerKey().ifPresent(storageOptions::setFilterBitsPerKey);
      builder.setSlatedbStorageOptions(storageOptions);
    });
    return builder.build();
  }
}
