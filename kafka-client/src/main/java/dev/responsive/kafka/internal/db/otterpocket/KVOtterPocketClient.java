package dev.responsive.kafka.internal.db.otterpocket;

public interface KVOtterPocketClient {

  byte[] get(int lssId, byte[] key);

  KVWALSegmentWriter writeSegment(int lssId, long endOffset, long currentEndOffset);

  interface KVWALSegmentWriter {
    void insert(byte[] key, byte[] val);

    void delete(byte[] key);

    void close();
  }
}
