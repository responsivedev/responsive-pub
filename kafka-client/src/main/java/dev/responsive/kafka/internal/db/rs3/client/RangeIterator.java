package dev.responsive.kafka.internal.db.rs3.client;

import java.util.Optional;

public interface RangeIterator {

  Optional<byte[]> lastKey();

  Optional<KeyValue> next();

}
