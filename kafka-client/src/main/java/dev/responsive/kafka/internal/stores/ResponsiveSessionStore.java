/*
 * Copyright 2023 Responsive Computing, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dev.responsive.kafka.internal.stores;

import static dev.responsive.kafka.internal.db.partitioning.SegmentPartitioner.UNINITIALIZED_STREAM_TIME;

import dev.responsive.kafka.api.config.ResponsiveConfig;
import dev.responsive.kafka.api.stores.ResponsiveSessionParams;
import dev.responsive.kafka.internal.utils.SessionKey;
import dev.responsive.kafka.internal.utils.TableName;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.errors.ProcessorStateException;
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

  private SessionOperations sessionOperations;

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
    final ResponsiveConfig responsiveConfig = ResponsiveConfig.responsiveConfig(appConfigs);

    this.context = storeContext;

    try {
      this.sessionOperations = SessionOperationsImpl.create(
          this.name,
          this.context,
          this.params,
          appConfigs,
          responsiveConfig,
          session -> session.sessionEndMs >= minValidEndTimestamp()
      );
    } catch (InterruptedException | TimeoutException e) {
      throw new ProcessorStateException("Failed to initialize store.", e);
    }

    log.info("Completed initializing state store");
    storeContext.register(root, this.sessionOperations);
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
    this.sessionOperations.close();
  }

  @Override
  public void put(final Windowed<Bytes> session, final byte[] aggregator) {
    // Write the aggregated value for the provided key to the store.
    if (session.window().end() < minValidEndTimestamp()) {
      return;
    }

    SessionKey sessionKey = new SessionKey(
        session.key(),
        session.window().start(),
        session.window().end()
    );
    if (aggregator == null) {
      this.sessionOperations.delete(sessionKey);
      return;
    }

    this.sessionOperations.put(sessionKey, aggregator);
    this.observedStreamTime = Math.max(this.observedStreamTime, sessionKey.sessionEndMs);
    StoreQueryUtils.updatePosition(this.position, this.context);
  }

  @Override
  public void remove(final Windowed<Bytes> session) {
    // Remove the session aggregated with provided Windowed key from the store
    // use sessionKey.window().start() and sessionKey.window.end().
    if (session.window().end() < minValidEndTimestamp()) {
      return;
    }

    SessionKey sessionKey = new SessionKey(
        session.key(),
        session.window().start(),
        session.window().end()
    );
    this.sessionOperations.delete(sessionKey);
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public byte[] fetchSession(
      final Bytes key,
      final long sessionStartTime,
      final long sessionEndTime
  ) {
    if (sessionEndTime < minValidEndTimestamp()) {
      return null;
    }

    SessionKey sessionKey = new SessionKey(
        key, sessionStartTime, sessionEndTime
    );
    return this.sessionOperations.fetch(sessionKey);
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
    // Fetch any sessions where end is ≥ earliestSessionEndTime and the sessions start
    // is ≤ latestSessionStartTime.
    // earliestSessionEndTime = Math.max(earliestSessionEndTime, minValidEndTimestamp());
    throw new UnsupportedOperationException("Not yet implemented");
  }

  /*
   * The minimum valid end timestamp of a session is going to depend on the last observed
   * stream time and the retention period stored in the parameters of the session store.
   */
  private long minValidEndTimestamp() {
    return this.observedStreamTime - this.params.retentionPeriod() + 1;
  }
}
