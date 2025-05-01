package dev.responsive.kafka.internal.db.rs3.client.jni;

import dev.responsive.kafka.internal.db.rs3.RS3ReaderClient;
import dev.responsive.kafka.internal.db.rs3.client.CurrentOffsets;
import dev.responsive.kafka.internal.db.rs3.client.LssId;
import dev.responsive.kafka.internal.db.rs3.client.PssCheckpoint;
import dev.responsive.kafka.internal.db.rs3.client.Range;
import dev.responsive.kafka.internal.db.rs3.client.RangeBound;
import dev.responsive.kafka.internal.db.rs3.client.StoreInfo;
import dev.responsive.kafka.internal.utils.WindowedKey;
import dev.responsive.rs3.jni.Bound;
import dev.responsive.rs3.jni.RS3Iterator;
import dev.responsive.rs3.jni.RS3Reader;
import java.nio.charset.Charset;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
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
  public void close() {
    rs3Reader.close();
  }

  private static class KVBoundMapper implements RangeBound.Mapper<Bytes, dev.responsive.rs3.jni.Bound> {

    @Override
    public Bound map(RangeBound.InclusiveBound<Bytes> b) {
      return dev.responsive.rs3.jni.Bound.inclusive(b.key().get());
    }

    @Override
    public Bound map(RangeBound.ExclusiveBound<Bytes> b) {
      return dev.responsive.rs3.jni.Bound.exclusive(b.key().get());
    }

    @Override
    public Bound map(RangeBound.Unbounded<Bytes> b) {
      return dev.responsive.rs3.jni.Bound.unbounded();
    }
  }

  private static class EmbeddedRs3Iterator<K, V, EK, EV> implements KeyValueIterator<K, V> {
    private final dev.responsive.rs3.jni.RS3Iterator<EK, EV> inner;
    private final Function<EK, K> keyMapper;
    private final Function<EV, V> valueMapper;
    private KeyValue<K, V> next;
    private boolean closed = false;

    private EmbeddedRs3Iterator(
        final RS3Iterator<EK, EV> inner,
        final Function<EK, K> keyMapper,
        final Function<EV, V> valueMapper
    ) {
      this.inner = inner;
      this.keyMapper = keyMapper;
      this.valueMapper = valueMapper;
      pollNext();
    }

    public static EmbeddedRs3Iterator<Bytes, byte[], byte[], byte[]> kvIterator(
        final dev.responsive.rs3.jni.RS3Iterator<byte[], byte[]> inner
    ) {
      return new EmbeddedRs3Iterator<>(
          inner,
          Bytes::wrap,
          v -> v
      );
    }

    @Override
    public void close() {
      if (!closed) {
        inner.close();
        closed = true;
      }
    }

    @Override
    public K peekNextKey() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      return next.key;
    }

    @Override
    public boolean hasNext() {
      return next != null;
    }

    @Override
    public KeyValue<K, V> next() {
      if (this.next == null) {
        throw new NoSuchElementException();
      }
      final var next = this.next;
      pollNext();
      return next;
    }

    private void pollNext() {
      next = inner.next()
          .map(r -> new KeyValue<>(keyMapper.apply(r.key()), valueMapper.apply(r.value())))
          .orElse(null);
    }
  }
}
