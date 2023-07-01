package dev.responsive.kafka.api;

import static dev.responsive.kafka.store.StoreUtils.durationToMillis;

import java.time.Duration;
import java.util.Objects;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.WindowStore;

public class WindowStoreOptions<K, V> {

  // required configs
  private final String name;
  private final long windowSize;

  // optional configs
  private long retentionMs;
  private boolean retainDuplicates = false;
  private boolean truncateChangelog = false;
  private Serde<K> keySerde;
  private Serde<V> valueSerde;

  /**
   * Get options for a basic state store with the given name and window size. By default
   * the retention period will be set to the window size and use overwrite semantics rather than
   * retaining duplicate copies.
   *
   * @param name the name to give this state store
   * @param windowSize the size of the windows (cannot be negative)
   * @return a new set of store options with the given parameters
   */
  public static <K, V> WindowStoreOptions<K, V> withNameAndSize(
      final String name,
      final Duration windowSize
  ) {
    Objects.requireNonNull(name, "name cannot be null");
    final long windowSizeMs = durationToMillis(windowSize, "windowSize");
    if (windowSizeMs < 0L) {
      throw new IllegalArgumentException("Invalid window size, must be non-negative");
    }
    return new WindowStoreOptions<>(name, windowSizeMs);
  }

  /**
   * Set the retention period for data in this {@link WindowStore}. By default, this is set to the
   * {@link #windowSize} and cannot be set smaller than that. Increasing this means windows will
   * be kept around even after they have closed, which may be useful for applications that use
   * interactive queries to view a state store's contents outside of the processor.
   *
   * @param retentionPeriod how long to hold windows, must be greater or equal to window size
   * @return itself
   */
  public WindowStoreOptions<K, V> withRetention(final Duration retentionPeriod) {
    final long retentionMs = durationToMillis(retentionPeriod, "retentionPeriod");
    if (retentionMs < windowSize) {
      throw new IllegalArgumentException("Invalid retention period, must be at least window size");
    }
    this.retentionMs = retentionMs;
    return this;
  }

  /**
   * Set whether the store should keep duplicates instead of overwriting values. Turning this on
   * will automatically disable caching and means that null values will be ignored.
   *
   * @param retainDuplicates whether to retain duplicates
   * @return itself
   */
  public WindowStoreOptions<K, V> withRetainDuplicates(final boolean retainDuplicates) {
    this.retainDuplicates = retainDuplicates;
    return this;
  }

  /**
   * Set the keySerde this {@link StateStore} will use.
   *
   * @param keySerde  the key {@link Serde} to use. If null, then the default key serde from the 
   *                  Streams configs will be used.
   * @return itself
   */
  public WindowStoreOptions<K, V> withKeySerde(final Serde<K> keySerde) {
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
  public WindowStoreOptions<K, V> withValueSerde(final Serde<V> valueSerde) {
    this.valueSerde = valueSerde;
    return this;
  }

  public String name() {
    return name;
  }

  public long windowSizeMs() {
    return windowSize;
  }

  public long retentionMs() {
    return retentionMs;
  }

  public boolean retainDuplicates() {
    return retainDuplicates;
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

  private WindowStoreOptions(final String name, final long windowSize) {
    this.name = name;
    this.windowSize = windowSize;
    this.retentionMs = windowSize;
  }

}
