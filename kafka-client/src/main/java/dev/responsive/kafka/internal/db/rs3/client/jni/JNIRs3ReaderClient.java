package dev.responsive.kafka.internal.db.rs3.client.jni;

import dev.responsive.kafka.internal.db.rs3.RS3ReaderClient;
import dev.responsive.kafka.internal.db.rs3.client.CurrentOffsets;
import dev.responsive.kafka.internal.db.rs3.client.LssId;
import dev.responsive.kafka.internal.db.rs3.client.PssCheckpoint;
import dev.responsive.kafka.internal.db.rs3.client.Range;
import dev.responsive.kafka.internal.db.rs3.client.StoreInfo;
import dev.responsive.kafka.internal.utils.WindowedKey;
import dev.responsive.rs3.jni.RS3Reader;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueIterator;

public class JNIRs3ReaderClient implements RS3ReaderClient {
  private final RS3Reader rs3Reader;

  public JNIRs3ReaderClient(
      final RS3Reader rs3Reader
  ) {
    this.rs3Reader = rs3Reader;
  }

  public static JNIRs3ReaderClient withConfig(final String config) {
    return JNIRs3ReaderClient.withConfig(config.getBytes(Charset.defaultCharset()));
  }

  public static JNIRs3ReaderClient withConfig(final byte[] config) {
    return new JNIRs3ReaderClient(RS3Reader.open(config));
  }

  public void loadPssReaderFromCheckpoint(
      final UUID storeReaderId,
      final PssCheckpoint pssCheckpoint
  ) {
    dev.responsive.rs3.jni.RS3PssCheckpoint jniCheckpoint
        = dev.responsive.rs3.jni.RS3PssCheckpoint.forSlateDbStorage(
            pssCheckpoint.pssId(),
            pssCheckpoint.slateDbStorageCheckpoint().get().path(),
            pssCheckpoint.slateDbStorageCheckpoint().get().checkpointId()
    );
    rs3Reader.loadPssReaderFromCheckpoint(storeReaderId, jniCheckpoint);
  }

  @Override
  public CurrentOffsets getCurrentOffsets(UUID storeReaderId, LssId lssId, int pssId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Optional<byte[]> get(UUID storeReaderId, LssId lssId, int pssId, Optional<Long> expectedWrittenOffset, Bytes key) {
    if (expectedWrittenOffset.isPresent()) {
      throw new UnsupportedOperationException();
    }
    return Optional.ofNullable(
        rs3Reader.kvGetValue(storeReaderId, lssId.id(), pssId, key.get()));
  }

  @Override
  public Optional<byte[]> windowedGet(UUID storeReaderId, LssId lssId, int pssId, Optional<Long> expectedWrittenOffset, WindowedKey key) {
    throw new UnsupportedOperationException();
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> range(UUID storeReaderId, LssId lssId, int pssId, Optional<Long> expectedWrittenOffset, Range<Bytes> range) {
    final var fromBound = range.start().map(new KVBoundMapper());
    final var toBound = range.end().map(new KVBoundMapper());
    if (expectedWrittenOffset.isPresent()) {
      throw new UnsupportedOperationException();
    }

    final var iterator = rs3Reader.kvGetRange(storeReaderId, lssId.id(), pssId, fromBound, toBound);
    return EmbeddedRs3Iterator.kvIterator(iterator);
  }

  @Override
  public KeyValueIterator<WindowedKey, byte[]> windowedRange(UUID storeReaderId, LssId lssId, int pssId, Optional<Long> expectedWrittenOffset, Range<WindowedKey> range) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<StoreInfo> listStores() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void closeStoreReader(final UUID storeId) {
    rs3Reader.closeStoreReader(storeId);
  }

  @Override
  public void close() {
    rs3Reader.close();
  }

}
