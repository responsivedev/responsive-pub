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

import static org.apache.kafka.streams.processor.internals.ProcessorContextUtils.asInternalProcessorContext;

import dev.responsive.kafka.api.stores.ResponsiveKeyValueParams;
import dev.responsive.kafka.internal.utils.TableName;
import java.util.List;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.Task.TaskType;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.internals.StoreQueryUtils;
import org.slf4j.Logger;

public class ResponsiveKeyValueStore
    implements KeyValueStore<Bytes, byte[]> {

  private final ResponsiveKeyValueParams params;
  private final TableName name;
  private final boolean isTimestamped;
  private final KVOperationsProvider opsProvider;

  private Position position; // TODO(IQ): update the position during restoration
  private boolean open;

  // All the fields below this are effectively final, we just can't set them until #init is called
  private Logger log;
  private KeyValueOperations operations;
  private StateStoreContext context;

  public ResponsiveKeyValueStore(
      final ResponsiveKeyValueParams params,
      final boolean isTimestamped
  ) {
    this(
        params,
        isTimestamped,
        ResponsiveKeyValueStore::provideOperations
    );
  }
 
  // Visible for Testing
  public ResponsiveKeyValueStore(
      final ResponsiveKeyValueParams params,
      final boolean isTimestamped,
      final KVOperationsProvider opsProvider
  ) {
    this.params = params;
    this.name = params.name();
    this.isTimestamped = isTimestamped;
    this.position = Position.emptyPosition();
    this.opsProvider = opsProvider;

    log = new LogContext(
        String.format("store [%s] ", name.kafkaName())
    ).logger(ResponsiveKeyValueStore.class);
  }

  @Override
  public String name() {
    return name.kafkaName();
  }

  @Override
  @Deprecated
  public void init(final ProcessorContext context, final StateStore root) {
    if (context instanceof StateStoreContext) {
      init((StateStoreContext) context, root);
    } else {
      throw new UnsupportedOperationException(
          "Use ResponsiveStore#init(StateStoreContext, StateStore) instead."
      );
    }
  }

  @Override
  public void init(final StateStoreContext storeContext, final StateStore root) {
    try {
      final TaskType taskType = asInternalProcessorContext(storeContext).taskType();
      log = new LogContext(
          String.format(
              "%sstore [%s] ",
              taskType == TaskType.GLOBAL ? "global-" : "",
              name.kafkaName())
      ).logger(ResponsiveKeyValueStore.class);

      log.info("Initializing state store");
      context = storeContext;

      if (taskType == TaskType.STANDBY) {
        log.warn("Unexpected standby task created, should transition to active shortly");
      }

      operations = opsProvider.provide(params, isTimestamped, storeContext, taskType);
      log.info("Completed initializing state store");

      open = true;
      storeContext.register(root, operations);
    } catch (final InterruptedException | TimeoutException e) {
      throw new ProcessorStateException("Failed to initialize store.", e);
    }
  }

  private static KeyValueOperations provideOperations(
      final ResponsiveKeyValueParams params,
      final boolean isTimestamped,
      final StateStoreContext context,
      final TaskType taskType
  ) throws InterruptedException, TimeoutException {
    return (taskType == TaskType.GLOBAL)
        ? GlobalOperations.create(context, params)
        : PartitionedOperations.create(params.name(), isTimestamped, context, params);
  }

  @Override
  public void flush() {
  }

  @Override
  public boolean isOpen() {
    return open;
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
  public void put(final Bytes key, final byte[] value) {
    if (value == null) {
      operations.delete(key);
    } else {
      operations.put(key, value);
    }
    StoreQueryUtils.updatePosition(position, context);
  }

  @Override
  public byte[] putIfAbsent(final Bytes key, final byte[] value) {
    final byte[] old = get(key);
    if (old == null && value != null) {
      put(key, value);
    }
    return old;
  }

  @Override
  public void putAll(final List<KeyValue<Bytes, byte[]>> entries) {
    entries.forEach(kv -> put(kv.key, kv.value));
  }

  @Override
  public byte[] delete(final Bytes key) {
    final byte[] delete = operations.delete(key);
    StoreQueryUtils.updatePosition(position, context);
    return delete;
  }

  @Override
  public byte[] get(final Bytes key) {
    return operations.get(key);
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> range(final Bytes from, final Bytes to) {
    return operations.range(from, to);
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> all() {
    return operations.all();
  }

  @Override
  public Position getPosition() {
    return position;
  }

  @Override
  public long approximateNumEntries() {
    return operations.approximateNumEntries();
  }

  @Override
  public void close() {
    if (operations != null) {
      operations.close();
    }
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> reverseRange(final Bytes from, final Bytes to) {
    return operations.reverseRange(from, to);
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> reverseAll() {
    return operations.reverseAll();
  }

}
