/*
 * Copyright 2025 Responsive Computing, Inc.
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
import dev.responsive.kafka.internal.db.rs3.client.Range;
import dev.responsive.kafka.internal.db.rs3.client.RangeBound;
import dev.responsive.kafka.internal.utils.WindowedKey;
import dev.responsive.rs3.Rs3;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;

public interface GrpcRangeKeyCodec<K extends Comparable<K>> {
  WindowedKeyCodec WINDOW_CODEC = new WindowedKeyCodec();
  BasicKeyCodec STANDARD_CODEC = new BasicKeyCodec();

  KeyValue<K, byte[]> decodeKeyValue(Rs3.KeyValue keyValue);

  Rs3.Range encodeRange(Range<K> range);

  class WindowedKeyCodec implements GrpcRangeKeyCodec<WindowedKey> {
    @Override
    public KeyValue<WindowedKey, byte[]> decodeKeyValue(final Rs3.KeyValue keyValue) {
      final var kvProto = keyValue.getWindowKv();
      final var keyProto = kvProto.getKey();

      final var key = new WindowedKey(
          Bytes.wrap(keyProto.getKey().toByteArray()),
          keyProto.getWindowTimestamp()
      );
      final var value = kvProto.getValue().toByteArray();
      return new KeyValue<>(key, value);
    }

    @Override
    public Rs3.Range encodeRange(final Range<WindowedKey> range) {
      final var rangeProto = Rs3.WindowRange.newBuilder();
      rangeProto.setFrom(boundProto(range.start()));
      rangeProto.setTo(boundProto(range.end()));
      return Rs3.Range.newBuilder()
          .setWindowRange(rangeProto)
          .build();
    }

    private Rs3.WindowBound boundProto(RangeBound<WindowedKey> bound) {
      final var boundProto = Rs3.WindowBound.newBuilder();
      return bound.map(new RangeBound.Mapper<>() {
        @Override
        public Rs3.WindowBound map(final RangeBound.InclusiveBound<WindowedKey> b) {
          final var key = Rs3.WindowKey.newBuilder()
              .setWindowTimestamp(b.key().windowStartMs)
              .setKey(ByteString.copyFrom(b.key().key.get()));
          return boundProto.setType(Rs3.BoundType.INCLUSIVE)
              .setKey(key)
              .build();
        }

        @Override
        public Rs3.WindowBound map(final RangeBound.ExclusiveBound<WindowedKey> b) {
          final var key = Rs3.WindowKey.newBuilder()
              .setWindowTimestamp(b.key().windowStartMs)
              .setKey(ByteString.copyFrom(b.key().key.get()));
          return boundProto.setType(Rs3.BoundType.EXCLUSIVE)
              .setKey(key)
              .build();
        }

        @Override
        public Rs3.WindowBound map(final RangeBound.Unbounded<WindowedKey> b) {
          return boundProto.setType(Rs3.BoundType.UNBOUNDED)
              .build();
        }
      });
    }
  }

  class BasicKeyCodec implements GrpcRangeKeyCodec<Bytes> {
    @Override
    public KeyValue<Bytes, byte[]> decodeKeyValue(final Rs3.KeyValue keyValue) {
      final var kvProto = keyValue.getBasicKv();
      final var key = Bytes.wrap(kvProto.getKey().getKey().toByteArray());
      final var value = kvProto.getValue().toByteArray();
      return new KeyValue<>(key, value);
    }

    @Override
    public Rs3.Range encodeRange(final Range<Bytes> range) {
      final var rangeProto = Rs3.BasicRange.newBuilder();
      rangeProto.setFrom(boundProto(range.start()));
      rangeProto.setTo(boundProto(range.end()));
      return Rs3.Range.newBuilder()
          .setBasicRange(rangeProto)
          .build();
    }

    private Rs3.BasicBound boundProto(RangeBound<Bytes> bound) {
      final var boundProto = Rs3.BasicBound.newBuilder();
      return bound.map(new RangeBound.Mapper<>() {
        @Override
        public Rs3.BasicBound map(final RangeBound.InclusiveBound<Bytes> b) {
          final var key = Rs3.BasicKey.newBuilder()
              .setKey(ByteString.copyFrom(b.key().get()));
          return boundProto.setType(Rs3.BoundType.INCLUSIVE)
              .setKey(key)
              .build();
        }

        @Override
        public Rs3.BasicBound map(final RangeBound.ExclusiveBound<Bytes> b) {
          final var key = Rs3.BasicKey.newBuilder()
              .setKey(ByteString.copyFrom(b.key().get()));
          return boundProto.setType(Rs3.BoundType.INCLUSIVE)
              .setKey(key)
              .build();
        }

        @Override
        public Rs3.BasicBound map(final RangeBound.Unbounded<Bytes> b) {
          return boundProto.setType(Rs3.BoundType.UNBOUNDED)
              .build();
        }
      });
    }
  }

}
