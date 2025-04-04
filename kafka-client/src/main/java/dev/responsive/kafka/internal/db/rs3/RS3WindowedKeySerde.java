package dev.responsive.kafka.internal.db.rs3;

import dev.responsive.kafka.internal.utils.WindowedKey;
import java.nio.ByteBuffer;
import org.apache.kafka.common.utils.Bytes;

public class RS3WindowedKeySerde {

  public byte[] serialize(final WindowedKey key) {
    final var result = new byte[key.key.get().length + 8];
    final var buffer = ByteBuffer.wrap(result);
    buffer.put(key.key.get());
    buffer.putLong(key.windowStartMs);
    return result;
  }

  public WindowedKey deserialize(final byte[] bytes) {
    final var buffer = ByteBuffer.wrap(bytes);
    final var keyLength = bytes.length - 8;
    final var keyBytes = new byte[keyLength];
    buffer.get(keyBytes);
    final var windowStartMs = buffer.getLong();
    return new WindowedKey(Bytes.wrap(keyBytes), windowStartMs);
  }

}
