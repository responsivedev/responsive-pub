package dev.responsive.kafka.internal.stores;

import static dev.responsive.kafka.internal.db.partitioning.SegmentPartitioner.UNINITIALIZED_STREAM_TIME;

import dev.responsive.kafka.api.config.ResponsiveConfig;
import dev.responsive.kafka.api.stores.ResponsiveSessionParams;
import dev.responsive.kafka.internal.utils.TableName;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.internals.StoreQueryUtils;
import org.slf4j.Logger;

public class ResponsiveSessionStore implements SessionStore<Bytes, byte[]> {

  private final Logger log;
  private final ResponsiveSessionParams params;
  private final TableName name;

  private Position position; // TODO(IQ): update the position during restoration
  private boolean open;
  private StateStoreContext context;
  private long observedStreamTime = UNINITIALIZED_STREAM_TIME;

  public ResponsiveSessionStore(final ResponsiveSessionParams params) {
    this.name = params.name();
    this.params = params;
    this.position = Position.emptyPosition();
    this.log = new LogContext(
        String.format("session-store [%s] ", this.name.kafkaName())
    ).logger(ResponsiveWindowStore.class);
  }

  @Override
  @Deprecated
  public void init(final ProcessorContext context, final StateStore root) {
    if (context instanceof StateStoreContext) {
      init((StateStoreContext) context, root);
    } else {
      throw new UnsupportedOperationException(
          "Use ResponsiveWindowStore#init(StateStoreContext, StateStore) instead."
      );
    }
  }

  @Override
  public void init(final StateStoreContext storeContext, final StateStore root) {
    log.info("Initializing state store");

    final var appConfigs = storeContext.appConfigs();
    final ResponsiveConfig config = ResponsiveConfig.responsiveConfig(appConfigs);

    this.context = storeContext;

    // TODO: IMPLEMENT

    log.info("Completed initializing state store");

    this.open = true;
  }

  @Override
  public String name() {
    return this.name.kafkaName();
  }

  @Override
  public boolean persistent() {
    // Kafka Streams uses this to determine whether it
    // needs to create and lock state directories. since
    // the Responsive Client doesn't require flushing state
    // to disk, we return false even though the store is
    // persistent in a remote store
    return false;
  }

  @Override
  public boolean isOpen() {
    return this.open;
  }

  @Override
  public Position getPosition() {
    return this.position;
  }

  @Override
  public void flush() {
  }

  @Override
  public void close() {
    // TODO: IMPLEMENT
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public void put(final Windowed<Bytes> sessionKey, final byte[] aggregator) {
    // TODO: IMPLEMENT
    // Write the aggregated value for the provided key to the store
    StoreQueryUtils.updatePosition(this.position, this.context);
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public void remove(final Windowed<Bytes> sessionKey) {
    // TODO: IMPLEMENT
    // Remove the session aggregated with provided Windowed key from the store
    // use sessionKey.window().start() and sessionKey.window.end()
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(final Bytes key) {
    // TODO: IMPLEMENT
    // Retrieve all aggregated sessions for the provided key.
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(final Bytes from, final Bytes to) {
    // TODO: IMPLEMENT
    // Retrieve all aggregated sessions for the given range of keys.
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public KeyValueIterator<Windowed<Bytes>, byte[]> findSessions(
      Bytes key,
      long earliestSessionEndTime,
      long latestSessionEndTime
  ) {
    // TODO: IMPLEMENT
    // Fetch any sessions where end is ≥ earliestSessionEndTime and the sessions start
    // is ≤ latestSessionStartTime.
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public byte[] fetchSession(
      final Bytes key,
      final long sessionStartTime,
      final long sessionEndTime
  ) {
    throw new UnsupportedOperationException("Not yet implemented");
  }
}
