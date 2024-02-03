package dev.responsive.kafka.api.async;

import java.util.function.Supplier;
import org.apache.kafka.common.utils.Bytes;

public interface AsyncStore {
  ResponsiveFuture<Supplier<byte[]>> putIfAbsentAsync(Bytes key, byte[] value);

  ResponsiveFuture<byte[]> getAsync(Bytes key);
}
