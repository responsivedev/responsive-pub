package dev.responsive.kafka.internal.db.mongo;

import org.apache.kafka.common.utils.Bytes;

interface KeyCodec {
  Bytes decode(String key);

  String encode(Bytes key);
}
