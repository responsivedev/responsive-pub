package dev.responsive.kafka.api.async;

import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

public interface AsyncTimestampedKeyValueStore<K, V>
    extends TimestampedKeyValueStore<K, V>, AsyncKeyValueStore<K, ValueAndTimestamp<V>> {
}
