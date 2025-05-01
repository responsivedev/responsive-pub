package dev.responsive.kafka.internal.db.rs3;

import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

// placeholder/marker trait for RS3 read only kv stores
public interface RS3ReadOnlyKeyValueStore<K, V> extends ReadOnlyKeyValueStore<K, V> {
}
