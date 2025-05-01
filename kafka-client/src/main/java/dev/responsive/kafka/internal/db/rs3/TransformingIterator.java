package dev.responsive.kafka.internal.db.rs3;

import java.util.Objects;
import java.util.function.Function;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;

class TransformingIterator<K, WK, V, WV> implements KeyValueIterator<K, V> {
  private final KeyValueIterator<WK, WV> wrapped;
  private final Function<WK, K> keyTransformer;
  private final Function<WV, V> valueTransformer;

  public TransformingIterator(
      final KeyValueIterator<WK, WV> wrapped,
      final Function<WK, K> keyTransformer,
      final Function<WV, V> valueTransformer
  ) {
    this.wrapped = Objects.requireNonNull(wrapped);
    this.keyTransformer = Objects.requireNonNull(keyTransformer);
    this.valueTransformer = Objects.requireNonNull(valueTransformer);
  }

  @Override
  public void close() {
    wrapped.close();
  }

  @Override
  public K peekNextKey() {
    return keyTransformer.apply(wrapped.peekNextKey());
  }

  @Override
  public boolean hasNext() {
    return wrapped.hasNext();
  }

  @Override
  public KeyValue<K, V> next() {
    final var next = wrapped.next();
    return KeyValue.pair(
        keyTransformer.apply(next.key),
        valueTransformer.apply(next.value)
    );
  }
}
