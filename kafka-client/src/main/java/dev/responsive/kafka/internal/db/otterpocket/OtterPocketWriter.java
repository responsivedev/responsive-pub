package dev.responsive.kafka.internal.db.otterpocket;

import dev.responsive.kafka.internal.db.RemoteWriter;
import dev.responsive.kafka.internal.db.otterpocket.KVOtterPocketClient.KVWALSegmentWriter;
import dev.responsive.kafka.internal.stores.RemoteWriteResult;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.apache.kafka.common.utils.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OtterPocketWriter<K> implements RemoteWriter<Bytes, Integer> {
  private static final Logger LOGGER = LoggerFactory.getLogger(OtterPocketWriter.class);

  private final KVOtterPocketClient.KVWALSegmentWriter segmentWriter;
  private final int lssId;

  public OtterPocketWriter(final KVWALSegmentWriter segmentWriter, final int lssId) {
    this.segmentWriter = Objects.requireNonNull(segmentWriter);
    this.lssId = lssId;
  }

  @Override
  public void insert(final Bytes key, final byte[] value, final long epochMillis) {
    segmentWriter.insert(key.get(), value);
  }

  @Override
  public void delete(final Bytes key) {
    segmentWriter.delete(key.get());
  }

  @Override
  public CompletionStage<RemoteWriteResult<Integer>> flush() {
    try {
      segmentWriter.close();
      return CompletableFuture.completedFuture(RemoteWriteResult.success(lssId));
    } catch (final RuntimeException e) {
      LOGGER.error("failed to write segment to {}", lssId, e);
      return CompletableFuture.completedFuture(RemoteWriteResult.failure(lssId));
    }
  }
}
