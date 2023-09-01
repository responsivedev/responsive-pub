package dev.responsive.kafka.store;

import dev.responsive.kafka.api.ResponsiveKeyValueParams;
import org.apache.kafka.streams.state.TimestampedBytesStore;

/**
 * Timestamped version of the {@link ResponsiveStore} that just implements the
 * {@link TimestampedBytesStore} marker interface.
 *
 * Since we don't have to worry about state being migrated from a non-timestamped to timestamped
 * store, this class doesn't have to do anything special -- Kafka Streams will handle all the
 * wrapping and unwrapping of the {@link org.apache.kafka.streams.state.ValueAndTimestamp} so the
 * underlying Responsive store only sees bytes in the end. This exists solely as a marker to tell
 * Streams it can be used to store timestamps.
 */
public class ResponsiveTimestampedStore extends ResponsiveStore implements TimestampedBytesStore {

  public ResponsiveTimestampedStore(final ResponsiveKeyValueParams params) {
    super(params);
  }
}
