package dev.responsive.kafka.internal.db.mongo;

import org.apache.kafka.common.utils.Bytes;

public class StringKeyCodec implements KeyCodec {
  private final OrderPreservingBase64Encoder encoder = new OrderPreservingBase64Encoder();

  @Override
  public Bytes decode(final String key) {
    return Bytes.wrap(encoder.decode(key));
  }

  @Override
  public String encode(Bytes key) {
    return encoder.encode(key.get());
  }
}
