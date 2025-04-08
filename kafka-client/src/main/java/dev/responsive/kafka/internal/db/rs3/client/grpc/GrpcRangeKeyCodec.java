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
import dev.responsive.kafka.internal.utils.WindowedKey;
import dev.responsive.rs3.Rs3;
import org.apache.kafka.common.utils.Bytes;

public interface GrpcRangeKeyCodec<K> {
  WindowedKeyCodec WINDOW_CODEC = new WindowedKeyCodec();
  StandardKeyCodec STANDARD_CODEC = new StandardKeyCodec();

  K decodeRangeResult(Rs3.KeyValue keyValue);

  void encodeRangeBound(K key, Rs3.Bound.Builder builder);

  class WindowedKeyCodec implements GrpcRangeKeyCodec<WindowedKey> {
    @Override
    public WindowedKey decodeRangeResult(final Rs3.KeyValue keyValue) {
      final var key = Bytes.wrap(keyValue.getKey().toByteArray());
      final var windowTimestamp = keyValue.getWindowTimestamp();
      return new WindowedKey(key, windowTimestamp);
    }

    @Override
    public void encodeRangeBound(final WindowedKey key, final Rs3.Bound.Builder builder) {
      builder.setKey(ByteString.copyFrom(key.key.get()));
      builder.setWindowTimestamp(key.windowStartMs);
    }
  }

  class StandardKeyCodec implements GrpcRangeKeyCodec<Bytes> {
    @Override
    public Bytes decodeRangeResult(final Rs3.KeyValue keyValue) {
      return Bytes.wrap(keyValue.getKey().toByteArray());
    }

    @Override
    public void encodeRangeBound(final Bytes key, final Rs3.Bound.Builder builder) {
      builder.setKey(ByteString.copyFrom(key.get()));
    }
  }
}
