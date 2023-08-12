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

package dev.responsive.kafka.store;

import static org.apache.kafka.streams.processor.internals.ProcessorContextUtils.asInternalProcessorContext;
import static org.apache.kafka.streams.processor.internals.ProcessorContextUtils.changelogFor;

import dev.responsive.db.BytesKeySpec;
import dev.responsive.db.CassandraClient;
import dev.responsive.db.RemoteKeyValueSchema;
import dev.responsive.db.partitioning.SubPartitioner;
import dev.responsive.kafka.api.InternalConfigs;
import dev.responsive.kafka.api.ResponsiveKeyValueParams;
import dev.responsive.kafka.clients.SharedClients;
import dev.responsive.kafka.config.ResponsiveConfig;
import dev.responsive.model.Result;
import dev.responsive.utils.RemoteMonitor;
import dev.responsive.utils.StoreUtil;
import dev.responsive.utils.TableName;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.Task.TaskType;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.internals.StoreQueryUtils;
import org.slf4j.Logger;

public class ResponsivePartitionedStore implements KeyValueStore<Bytes, byte[]> {

  private Logger log;

  private final TableName name;
  private final Position position;
  private final ResponsiveKeyValueParams params;
  private ResponsiveStoreRegistry storeRegistry;
  private ResponsiveStoreRegistration registration;

  private boolean open;
  private SubPartitioner partitioner;
  private CommitBuffer<Bytes, RemoteKeyValueSchema> buffer;

  @SuppressWarnings("rawtypes")
  private InternalProcessorContext context;
  private TopicPartition topicPartition;
  private RemoteKeyValueSchema schema;

  public ResponsivePartitionedStore(final ResponsiveKeyValueParams params) {
    this.params = params;
    this.name = params.name();
    this.position = Position.emptyPosition();
    log = new LogContext(
        String.format("store [%s]", name.kafkaName())
    ).logger(ResponsivePartitionedStore.class);
  }

  @Override
  public String name() {
    return name.kafkaName();
  }

  @Override
  @SuppressWarnings("deprecation")
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
  public void init(final StateStoreContext context, final StateStore root) {
    try {
      this.context = asInternalProcessorContext(context);
      storeRegistry = InternalConfigs.loadStoreRegistry(context.appConfigs());
      topicPartition =  new TopicPartition(
          changelogFor(context, name.kafkaName(), false),
          context.taskId().partition()
      );

      if (!isActive()) {
        log = new LogContext(
            String.format("standby-store [%s]", name.kafkaName())
        ).logger(ResponsivePartitionedStore.class);
      }

      log.info("Initializing state store");

      StoreUtil.validateTopologyOptimizationConfig(context.appConfigs());
      final ResponsiveConfig config = new ResponsiveConfig(context.appConfigs());

      final SharedClients sharedClients = new SharedClients(context.appConfigs());
      CassandraClient client = sharedClients.cassandraClient;

      schema = client.kvSchema(params.schemaType());
      schema.create(params.name().cassandraName(), params.timeToLive());

      final RemoteMonitor monitor = client.awaitTable(name.cassandraName(), sharedClients.executor);
      monitor.await(Duration.ofSeconds(60));
      log.info("Remote table {} is available for querying.", name.cassandraName());

      schema.prepare(name.cassandraName());

      partitioner = params.schemaType() == SchemaType.FACT
          ? SubPartitioner.NO_SUBPARTITIONS
          : config.getSubPartitioner(sharedClients.admin, name, topicPartition.topic());

      buffer = CommitBuffer.from(
          sharedClients,
          name,
          topicPartition,
          schema,
          new BytesKeySpec(),
          partitioner,
          config
      );

      if (isActive()) {
        buffer.init();
        registration = new ResponsiveStoreRegistration(
            name.kafkaName(),
            topicPartition,
            buffer::flush,
            this::storedOffset
        );
      } else {
        registration = new ResponsiveStoreRegistration(
            name.kafkaName(),
            topicPartition,
            buffer::flush,
            this::storedOffset,
            this::maybeTransitionToActive
        );
      }

      storeRegistry.registerStore(registration);

      open = true;
      context.register(
          root,
          new ActiveFilteringRestoreCallback(
              topicPartition,
              (batch) -> buffer.restoreBatch(batch),
              this::isActive
          )
      );
    } catch (InterruptedException | TimeoutException e) {
      throw new ProcessorStateException("Failed to initialize store " + name(), e);
    }
  }

  private boolean isActive() {
    return context.taskType() == TaskType.ACTIVE;
  }

  /**
   * The current stored offset in the remote table. Guaranteed to be less than or equal to the
   * committed offset of the corresponding changelog topic partition in Kafka
   * <p>
   * NOTE: this always makes a remote call, should be cached eg when used to obtain starting offset
   *
   * @return the latest offset stored in the remote table, or
   *         {@link ResponsiveStoreRegistration#NO_COMMITTED_OFFSET} if no offset was yet committed
   */
  private long storedOffset() {
    if (isActive()) {
      return buffer.offset();
    } else {
      log.error("Tried to access the stored offset but the commit buffer is still uninitialized");
      throw new IllegalStateException("Requested stored offset before buffer initialization");
    }
  }

  /**
   * Due to a "bug" in Kafka Streams before version 3.6, it's possible for temporary standby tasks
   * to be created so they can be recycled into active tasks in an immediate followup rebalance.
   * This is a unique "bug" arising when an active task must be revoked from another client before
   * it can be assigned to this (for which a standby is placed to avoid closing and losing in-memory
   * state), thus we do not need to worry about the reverse case of active tasks becoming standby
   * to worry about tasks being.
   * <p>
   * Standby tasks create a problem for us because we can't initialize the commit buffer without
   * accidentally fencing the active task. We must delay the buffer initialization until the standby
   * task is recycled into an active. This method is invoked to complete the transition
   * <p>
   * A recycled task will close everything except for its state stores, which are handed over as-is
   * to the new task without going through #close or #init again. We use the restore consumer and a
   * restore listener to infer when a task transitions from standby to active restoration. We also
   * block all restoration while in standby mode, and wait to set the starting offset until it's
   * recycled to ensure we don't overcount or undercount any changelog records
   *
   * @return whether the store finished initialization and is ready to restore as an active task
   */
  // TODO extract this and below methods into common superclass to share with Window/Session stores
  private boolean maybeTransitionToActive() {
    if (isActive()) {
      log.info("Transitioning from standby to active");

      log = new LogContext(
          String.format("store [%s]", name.kafkaName())
      ).logger(ResponsivePartitionedStore.class);

      buffer.init();
      log.info("Initialized buffer");
      return true;
    } else {
      log.debug("Skipping initialization for restore start in standby mode");
      return false;
    }
  }

  private void enforceIsActive(final String caller) {
    if (!isActive()) {
      log.error("Invoked {} on store while in STANDBY", caller);
      throw new IllegalStateException("Unexpected read/write to store in standby state: " + name());
    }
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
    enforceIsActive("put");
    buffer.put(key, value, context.timestamp());
    StoreQueryUtils.updatePosition(position, context);
  }

  @Override
  public byte[] putIfAbsent(final Bytes key, final byte[] value) {
    // since there's only a single writer, we don't need to worry
    // about the concurrency aspects here (e.g. it's not possible
    // that between the get(key) and put(key, value) another write
    // comes in unless this client is fenced, in which case this
    // batch will not be committed to remote storage)
    final byte[] old = get(key);
    if (old == null) {
      put(key, value);
    }
    return old;
  }

  @Override
  public void putAll(final List<KeyValue<Bytes, byte[]>> entries) {
    entries.forEach(kv -> put(kv.key, kv.value));
    StoreQueryUtils.updatePosition(position, context);
  }

  @Override
  public byte[] delete(final Bytes key) {
    // single writer prevents races (see putIfAbsent)
    final byte[] old = get(key);
    buffer.tombstone(key, context.timestamp());
    return old;
  }

  @Override
  public byte[] get(final Bytes key) {
    enforceIsActive("get");
    // try the buffer first, it acts as a local cache
    // but this is also necessary for correctness as
    // it is possible that the data is either uncommitted
    // or not yet pushed to the remote store
    final Result<Bytes> result = buffer.get(key);
    if (result != null) {
      return result.isTombstone ? null : result.value;
    }

    final int subPartition = partitioner.partition(topicPartition.partition(), key);
    final long minValidTs = params
        .timeToLive()
        .map(ttl -> context.timestamp() - ttl.toMillis())
        .orElse(-1L);

    return schema.get(name.cassandraName(), subPartition, key, minValidTs);
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> range(final Bytes from, final Bytes to) {
    enforceIsActive("range");
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> all() {
    enforceIsActive("all");
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  @Override
  public Position getPosition() {
    return position;
  }

  @Override
  public long approximateNumEntries() {
    return partitioner
        .all(topicPartition.partition())
        .mapToLong(p -> schema.cassandraClient().count(name.cassandraName(), p))
        .sum();
  }

  @Override
  public void close() {
    // no need to flush the buffer here, will happen through the kafka client commit as usual
    if (storeRegistry != null) {
      storeRegistry.deregisterStore(registration);
    }
    open = false;
  }
}
