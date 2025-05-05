package dev.responsive.kafka.internal.db.rs3.client.jni;

import dev.responsive.rs3.jni.RS3Iterator;
import java.util.NoSuchElementException;
import java.util.function.Function;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;

class EmbeddedRs3Iterator<K, V, EK, EV> implements KeyValueIterator<K, V> {
  private final dev.responsive.rs3.jni.RS3Iterator<EK, EV> inner;
  private final Function<EK, K> keyMapper;
  private final Function<EV, V> valueMapper;
  private KeyValue<K, V> next;
  private boolean closed = false;

  EmbeddedRs3Iterator(
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
      final RS3Iterator<byte[], byte[]> inner
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
