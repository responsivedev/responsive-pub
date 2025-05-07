package dev.responsive.kafka.internal.db.rs3;

import java.util.Objects;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueIterator;

public class RS3ReadOnlyDeserializingKeyValueStore<K, V>
    implements RS3ReadOnlyKeyValueStore<K, V> {
  private final RS3ReadOnlyKeyValueStore<Bytes, byte[]> wrapped;
  private final Serde<K> keySerde;
  private final Deserializer<V> valueDeserializer;

  RS3ReadOnlyDeserializingKeyValueStore(
      final RS3ReadOnlyKeyValueStore<Bytes, byte[]> wrapped,
      final Serde<K> keySerde,
      final Deserializer<V> valueDeserializer
  ) {
    this.wrapped = Objects.requireNonNull(wrapped);
    this.keySerde = Objects.requireNonNull(keySerde);
    this.valueDeserializer = Objects.requireNonNull(valueDeserializer);
  }

  @Override
  public V get(K key) {
    return deserializeValue(
        wrapped.get(
            serializeKey(key)
        )
    );
  }

  @Override
  public KeyValueIterator<K, V> range(K from, K to) {
    final var fromBytes = serializeKey(from);
    final var toBytes = serializeKey(to);
    return new TransformingIterator<>(
        wrapped.range(fromBytes, toBytes),
        this::deserializeKey,
        this::deserializeValue
    );
  }

  @Override
  public KeyValueIterator<K, V> all() {
    return new TransformingIterator<>(
        wrapped.all(),
        this::deserializeKey,
        this::deserializeValue
    );
  }

  @Override
  public long approximateNumEntries() {
    return wrapped.approximateNumEntries();
  }

  public static <K, V> RS3ReadOnlyKeyValueStore<K, V> forTimestamped(
      final RS3ReadOnlyKeyValueStore<Bytes, byte[]> wrapped,
      final Serde<K> keySerde,
      final Serde<V> valueSerde
  ) {
    return new RS3ReadOnlyDeserializingKeyValueStore<>(
        wrapped,
        keySerde,
        new ValueAndTimestampDeserializerAdaptor<>(valueSerde)
    );
  }

  public static <K, V> RS3ReadOnlyKeyValueStore<K, V> forKeyValue(
      final RS3ReadOnlyKeyValueStore<Bytes, byte[]> wrapped,
      final Serde<K> keySerde,
      final Serde<V> valueSerde
  ) {
    return new RS3ReadOnlyDeserializingKeyValueStore<>(
        wrapped,
        keySerde,
        valueSerde.deserializer()
    );
  }

  private Bytes serializeKey(final K key) {
    return Bytes.wrap(keySerde.serializer().serialize("", key));
  }

  private K deserializeKey(final Bytes key) {
    return keySerde.deserializer().deserialize("", key.get());
  }

  private V deserializeValue(final byte[] value) {
    return value == null ? null : valueDeserializer.deserialize("", value);
  }
}
