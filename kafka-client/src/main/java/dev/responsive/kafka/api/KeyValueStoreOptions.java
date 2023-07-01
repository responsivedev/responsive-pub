package dev.responsive.kafka.api;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * Configuration options for building a Responsive {@link KeyValueStore}. Use {@link #named(String)}
 * (or {@link #namedGlobal(String)} if this is a global store) to get a new object and pass that in
 * to one of the {@link ResponsiveStoreFactory} APIs, or apply any of the additional optional
 * configs via the {@code #withXXX} methods below.
 */
public class KeyValueStoreOptions<K, V> {

  // required configs
  private final String name;
  private final boolean isGlobal;

  // optional configs
  private boolean truncateChangelog = false;
  private Long ttlMs = null;
  private Serde<K> keySerde;
  private Serde<V> valueSerde;

  /**
   * Get options for a basic state store with the given name. If this is a global state store,
   * use {@link #namedGlobal(String)} instead.
   *
   * @param name the name to give this state store
   * @return a new set of store options with the given name
   */
  public static <K, V> KeyValueStoreOptions<K, V> named(final String name) {
    return new KeyValueStoreOptions<>(name, false);
  }

  /**
   * Get options for a global state store with the given name. Only valid for
   * {@link org.apache.kafka.streams.state.KeyValueStore key-value stores}
   *
   * @param name the name to give this state store
   * @return a new set of global store options with the given name
   */
  public static <K, V> KeyValueStoreOptions<K, V> namedGlobal(final String name) {
    return new KeyValueStoreOptions<>(name, true);
  }

  /**
   * Set the keySerde this {@link StateStore} will use.
   *
   * @param keySerde  the key {@link Serde} to use. If null, then the default key serde from the
   *                  Streams configs will be used.
   * @return itself
   */
  public KeyValueStoreOptions<K, V> withKeySerde(final Serde<K> keySerde) {
    this.keySerde = keySerde;
    return this;
  }

  /**
   * Set the valueSerde the {@link StateStore} will use.
   *
   * @param valueSerde the value {@link Serde} to use. If null, then the default key serde from the
   *                   Streams configs will be used. If the serialized bytes are null for put
   *                   operations, it is treated as a delete operation.
   * @return itself
   */
  public KeyValueStoreOptions<K, V> withValueSerde(final Serde<V> valueSerde) {
    this.valueSerde = valueSerde;
    return this;
  }

  public Long ttl() {
    return ttlMs;
  }

  public boolean truncateChangelog() {
    return truncateChangelog;
  }

  public Serde<K> keySerde() {
    return keySerde;
  }

  public Serde<V> valueSerde() {
    return valueSerde;
  }

  private KeyValueStoreOptions(final String name, final boolean isGlobal) {
    this.name = name;
    this.isGlobal = isGlobal;
  }
}
