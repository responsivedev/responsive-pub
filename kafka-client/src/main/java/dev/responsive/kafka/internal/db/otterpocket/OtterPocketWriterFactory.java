package dev.responsive.kafka.internal.db.otterpocket;

import dev.responsive.kafka.internal.db.RemoteWriter;
import dev.responsive.kafka.internal.db.WriterFactory;
import dev.responsive.kafka.internal.stores.RemoteWriteResult;
import java.util.Objects;
import org.apache.kafka.common.utils.Bytes;

public class OtterPocketWriterFactory extends WriterFactory<Bytes, Integer> {
  private final KVOtterPocketClient opClient;
  private final int lssId;

  public OtterPocketWriterFactory(
      final String logPrefix,
      final KVOtterPocketClient opClient,
      final int lssId
  ) {
    super(logPrefix);
    this.opClient = Objects.requireNonNull(opClient);
    this.lssId = lssId;
  }


  @Override
  public RemoteWriter<Bytes, Integer> createWriter(final Integer tablePartition) {
    return new OtterPocketWriter<>(
        // TODO: pass through offsets here
        opClient.writeSegment(tablePartition, 0, 0),
        tablePartition
    );
  }

  @Override
  public String tableName() {
    return null;
  }

  @Override
  protected Integer tablePartitionForKey(final Bytes key) {
    return lssId;
  }

  @Override
  protected RemoteWriteResult<Integer> setOffset(final long consumedOffset) {
    // todo: implement offset tracking
    return RemoteWriteResult.success(lssId);
  }

  @Override
  protected long offset() {
    // todo: implement offset tracking
    return 0;
  }
}
