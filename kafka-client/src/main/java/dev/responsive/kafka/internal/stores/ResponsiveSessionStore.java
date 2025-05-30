/*
 * Copyright 2024 Responsive Computing, Inc.
 *
 * This source code is licensed under the Responsive Business Source License Agreement v1.0
 * available at:
 *
 * https://www.responsive.dev/legal/responsive-bsl-10
 *
 * This software requires a valid Commercial License Key for production use. Trial and commercial
 * licenses can be obtained at https://www.responsive.dev
 */

package dev.responsive.kafka.internal.stores;


import static dev.responsive.kafka.internal.db.partitioning.Segmenter.UNINITIALIZED_STREAM_TIME;
import static org.apache.kafka.streams.processor.internals.ProcessorContextUtils.asInternalProcessorContext;

import dev.responsive.kafka.api.config.ResponsiveConfig;
import dev.responsive.kafka.api.stores.ResponsiveSessionParams;
import dev.responsive.kafka.internal.utils.Iterators;
import dev.responsive.kafka.internal.utils.SessionKey;
import dev.responsive.kafka.internal.utils.TableName;
import java.util.Collection;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.RecordBatchingStateRestoreCallback;
import org.apache.kafka.streams.processor.internals.Task.TaskType;
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
    ).logger(ResponsiveSessionStore.class);
  }

  @Override
  public void init(final StateStoreContext storeContext, final StateStore root) {
    log.info("Initializing state store");

    final var appConfigs = storeContext.appConfigs();
    final ResponsiveConfig responsiveConfig = ResponsiveConfig.responsiveConfig(appConfigs);

    this.context = storeContext;

    final TaskType taskType = asInternalProcessorContext(storeContext).taskType();
    if (taskType == TaskType.STANDBY) {
      log.error("Unexpected standby task created");
      throw new IllegalStateException("Store " + name() + " was opened as a standby");
    }

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
    storeContext.register(root, (RecordBatchingStateRestoreCallback) this::restoreBatch);
    this.open = true;
  }

  @Override
  public String name() {
    return name.kafkaName();
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
    return open;
  }

  @Override
  public Position getPosition() {
    return position;
  }

  @Override
  public void flush() {
  }

  @Override
  public void close() {
    sessionOperations.close();
  }

  @Override
  public void put(final Windowed<Bytes> session, final byte[] aggregator) {
    if (session.window().end() < minValidEndTimestamp()) {
      return;
    }

    final SessionKey sessionKey = new SessionKey(
        session.key(),
        session.window().start(),
        session.window().end()
    );
    if (aggregator == null) {
      sessionOperations.delete(sessionKey);
      return;
    }

    sessionOperations.put(sessionKey, aggregator);
    observedStreamTime = Math.max(observedStreamTime, sessionKey.sessionEndMs);
    StoreQueryUtils.updatePosition(position, context);
  }

  @Override
  public void remove(final Windowed<Bytes> session) {
    if (session.window().end() < minValidEndTimestamp()) {
      return;
    }

    final SessionKey sessionKey = new SessionKey(
        session.key(),
        session.window().start(),
        session.window().end()
    );
    sessionOperations.delete(sessionKey);
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

    final SessionKey sessionKey = new SessionKey(
        key, sessionStartTime, sessionEndTime
    );
    return sessionOperations.fetch(sessionKey);
  }

  @Override
  public KeyValueIterator<Windowed<Bytes>, byte[]> findSessions(
      final Bytes key,
      final long earliestSessionEndTime,
      final long latestSessionStartTime
  ) {
    // Trim down our search space by using both the observed stream time.
    final long actualEarliestEndTime = Long.max(
        0,
        Long.max(earliestSessionEndTime, minValidEndTimestamp())
    );
    final long actualLatestEndTime = Long.max(observedStreamTime, 0);

    final var sessions = sessionOperations.fetchAll(key,
        actualEarliestEndTime, actualLatestEndTime
    );

    return Iterators.filterKv(sessions, session -> {
      return session.window().start() <= latestSessionStartTime
          && session.window().end() >= actualEarliestEndTime;
    });
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

  /*
   * The minimum valid end timestamp of a session is going to depend on the last observed
   * stream time and the retention period stored in the parameters of the session store.
   */
  private long minValidEndTimestamp() {
    return observedStreamTime - params.retentionPeriod() + 1;
  }

  public void restoreBatch(final Collection<ConsumerRecord<byte[], byte[]>> records) {
    observedStreamTime = Math.max(
        observedStreamTime,
        sessionOperations.restoreBatch(records, observedStreamTime)
    );
  }
}
