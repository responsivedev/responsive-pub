package dev.responsive.kafka.internal.db.otterpocket;

public interface OtterPocketClient<QT, CT> {
  // TODO: change to generic query and change API (e.g. decouple rocksdb-cloud)
  byte[] query(int lssId, QT query);

  WALSegmentWriter<CT> writeSegment(int lssId, long endOffset, long currentEndOffset);

  interface WALSegmentWriter<CT> {
    void write(CT change);

    void close();
  }
}
