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

import static org.apache.kafka.streams.processor.internals.ProcessorContextUtils.asInternalProcessorContext;

import dev.responsive.kafka.api.stores.ResponsiveKeyValueParams;
import dev.responsive.kafka.internal.utils.TableName;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.Task.TaskType;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.streams.state.internals.StoreAccessorUtil;
import org.apache.kafka.streams.state.internals.StoreQueryUtils;
import org.slf4j.Logger;

public class ResponsiveKeyValueStore
    implements KeyValueStore<Bytes, byte[]> {

  private final ResponsiveKeyValueParams params;
  private final TableName name;
  private final KVOperationsProvider opsProvider;

  private Position position; // TODO(IQ): update the position during restoration
  private boolean open;

  // All the fields below this are effectively final, we just can't set them until #init is called
  private Logger log;
  private KeyValueOperations operations;
  private StateStoreContext context;

  public ResponsiveKeyValueStore(
      final ResponsiveKeyValueParams params
  ) {
    this(
        params,
        ResponsiveKeyValueStore::provideOperations
    );
  }
 
  // Visible for Testing
  public ResponsiveKeyValueStore(
      final ResponsiveKeyValueParams params,
      final KVOperationsProvider opsProvider
  ) {
    this.params = params;
    this.name = params.name();
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
        log.error("Unexpected standby task created");
        throw new IllegalStateException("Store " + name() + " was opened as a standby");
      }

      final StateSerdes<?, ?> stateSerdes = StoreAccessorUtil.extractKeyValueStoreSerdes(root);
      final Optional<TtlResolver<?, ?>> ttlResolver = TtlResolver.fromTtlProviderAndStateSerdes(
          stateSerdes,
          params.ttlProvider()
      );

      operations = opsProvider.provide(params, ttlResolver, storeContext, taskType);
      log.info("Completed initializing state store");

      open = true;
      storeContext.register(root, operations);
    } catch (final InterruptedException | TimeoutException e) {
      throw new ProcessorStateException("Failed to initialize store.", e);
    }
  }

  private static KeyValueOperations provideOperations(
      final ResponsiveKeyValueParams params,
      final Optional<TtlResolver<?, ?>> ttlResolver,
      final StateStoreContext context,
      final TaskType taskType
  ) throws InterruptedException, TimeoutException {
    return (taskType == TaskType.GLOBAL)
        ? GlobalOperations.create(context, params, ttlResolver)
        : PartitionedOperations.create(params.name(), ttlResolver, context, params);
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
  public <PS extends Serializer<P>, P> KeyValueIterator<Bytes, byte[]> prefixScan(
      final P prefix,
      final PS prefixKeySerializer
  ) {
    return operations.prefix(prefix, prefixKeySerializer);
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
