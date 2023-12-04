package dev.responsive.kafka.internal.db.otterpocket;

import com.google.protobuf.ByteString;
import dev.responsive.kafka.internal.db.otterpocket.OtterPocketClient.WALSegmentWriter;
import java.util.Objects;
import responsive.otterpocket.v1.service.Otterpockets.RocksDBCloudChange;
import responsive.otterpocket.v1.service.Otterpockets.RocksDBCloudQuery;

public class KVOtterPocketGrpcClient implements KVOtterPocketClient {
  private final OtterPocketGrpcClient<RocksDBCloudQuery, RocksDBCloudChange> opGrpc;

  public KVOtterPocketGrpcClient(
      final OtterPocketGrpcClient<RocksDBCloudQuery, RocksDBCloudChange> opGrpc) {
    this.opGrpc = Objects.requireNonNull(opGrpc);
  }

  @Override
  public byte[] get(final int lssId, final byte[] key) {
    return opGrpc.query(lssId, RocksDBCloudQuery.newBuilder()
        .setKey(ByteString.copyFrom(key))
        .build());
  }

  @Override
  public KVWALSegmentWriter writeSegment(
      final int lssId,
      final long endOffset,
      final long currentEndOffset
  ) {
    return new KVOtterPocketGrpcWALSegmentWriter(
        opGrpc.writeSegment(lssId, endOffset, currentEndOffset));
  }

  public static class KVOtterPocketGrpcWALSegmentWriter
      implements KVOtterPocketClient.KVWALSegmentWriter {
    private final WALSegmentWriter<RocksDBCloudChange> opSegmentWriter;

    public KVOtterPocketGrpcWALSegmentWriter(
        final WALSegmentWriter<RocksDBCloudChange> opSegmentWriter) {
      this.opSegmentWriter = Objects.requireNonNull(opSegmentWriter);
    }

    @Override
    public void insert(final byte[] key, final byte[] val) {
      opSegmentWriter.write(RocksDBCloudChange.newBuilder()
          .setKey(ByteString.copyFrom(key))
          .setValue(ByteString.copyFrom(val))
          .build());
    }

    @Override
    public void delete(final byte[] key) {
      opSegmentWriter.write(RocksDBCloudChange.newBuilder()
          .setValue(ByteString.copyFrom(key))
          .build());
    }

    @Override
    public void close() {
      opSegmentWriter.close();
    }
  }

}
