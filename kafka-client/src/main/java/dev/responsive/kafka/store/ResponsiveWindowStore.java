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

import dev.responsive.db.CassandraClient;
import dev.responsive.db.RemoteWindowedSchema;
import dev.responsive.db.StampedKeySpec;
import dev.responsive.db.partitioning.SubPartitioner;
import dev.responsive.kafka.api.InternalConfigs;
import dev.responsive.kafka.clients.SharedClients;
import dev.responsive.kafka.config.ResponsiveConfig;
import dev.responsive.model.Result;
import dev.responsive.model.Stamped;
import dev.responsive.utils.Iterators;
import dev.responsive.utils.RemoteMonitor;
import dev.responsive.utils.TableName;
import java.time.Duration;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.Task.TaskType;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.apache.kafka.streams.state.internals.StoreQueryUtils;
import org.slf4j.Logger;

public class ResponsiveWindowStore implements WindowStore<Bytes, byte[]> {

  private Logger log;

  private CassandraClient client;
  private RemoteWindowedSchema schema;

  private final TableName name;
  private final long windowSize;
  private final Position position;
  private final long retentionPeriod;

  private boolean open;
  private boolean initialized;

  @SuppressWarnings("rawtypes")
  private InternalProcessorContext context;
  private TopicPartition topicPartition;
  private CommitBuffer<Stamped<Bytes>, RemoteWindowedSchema> buffer;
  private long observedStreamTime;
  private ResponsiveStoreRegistry storeRegistry;
  private ResponsiveStoreRegistration registration;
  private SubPartitioner partitioner;

  public ResponsiveWindowStore(
      final TableName name,
      final long retentionPeriod,
      final long windowSize,
      final boolean retainDuplicates
  ) {
    this.name = name;
    log = new LogContext(
        String.format("window-store [%s]", name.kafkaName())
    ).logger(ResponsiveWindowStore.class);

    // TODO: figure out how to implement retention period in Cassandra
    // there are a few options for this: we can use the wall-clock based
    // TTL feature, but this would be a departure from how Kafka Streams
    // handles the retention period (based on stream time). Alternatively
    // we can post-filter from Cassandra and occasionally run a process
    // that cleans up expired records, this would work well in a background
    // process but we'd need to figure out where to run that - we could also
    // run these deletes asynchronously
    //
    // for now (so we can get correct behavior) we just post-filter anything
    // that is past the TTL
    this.retentionPeriod = retentionPeriod;
    this.windowSize = windowSize;
    this.position = Position.emptyPosition();

    if (retainDuplicates) {
      // TODO: we should implement support for retaining duplicates
      // I suspect this is a pretty niche use case, so this can wait for later
      // as it's only used to ensure the result of stream-stream joins include
      // duplicate results for the joins if there are duplicate keys in the source
      // with the same timestamp
      log.warn("ResponsiveWindowStore does not fully support retaining duplicates");
    }
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
          "Use ResponsiveWindowStore#init(StateStoreContext, StateStore) instead."
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
            String.format("standby-window-store [%s]", name.kafkaName())
        ).logger(ResponsivePartitionedStore.class);
      }

      log.info("Initializing state store");

      final ResponsiveConfig config = new ResponsiveConfig(context.appConfigs());

      final SharedClients sharedClients = new SharedClients(context.appConfigs());
      client = sharedClients.cassandraClient;

      schema = client.windowedSchema();
      schema.create(name.cassandraName());

      final RemoteMonitor monitor = client.awaitTable(name.cassandraName(), sharedClients.executor);
      monitor.await(Duration.ofSeconds(60));
      log.info("Remote table {} is available for querying.", name.cassandraName());

      schema.prepare(name.cassandraName());

      partitioner = config.getSubPartitioner(
          sharedClients.admin, name, topicPartition.topic());

      buffer = CommitBuffer.from(
          sharedClients,
          name,
          topicPartition,
          schema,
          new StampedKeySpec(this::withinRetention),
          config
      );

      if (isActive()) {
        initializeBuffer();
      }

      open = true;

      context.register(
          root,
          new ResponsiveRestoreCallback((batch) -> buffer.restoreBatch(batch), this::isActive)
      );
    } catch (InterruptedException | TimeoutException e) {
      throw new ProcessorStateException("Failed to initialize store.", e);
    }
  }

  private boolean isActive() {
    return context.taskType() == TaskType.ACTIVE;
  }

  private void initializeBuffer() {
    buffer.init();
    initialized = true;
    log.info("Initialized buffer");

    final long offset = buffer.offset();
    registration = new ResponsiveStoreRegistration(
        name.kafkaName(),
        topicPartition,
        offset == -1 ? 0 : offset,
        buffer::flush
    );
    storeRegistry.registerStore(registration);
  }

  private void maybeTransitionToActive(final String caller) {
    if (isActive()) {
      if (!initialized) {
        log.info("Transitioning from standby to active");

        log = new LogContext(
            String.format("window-store [%s]", name.kafkaName())
        ).logger(ResponsivePartitionedStore.class);

        initializeBuffer();
      }
    } else {
      log.error("Invoked {} on store while in STANDBY", caller);
      throw new IllegalStateException("Unexpected read/write to store in standby state: " + name());
    }
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
  public void put(final Bytes key, final byte[] value, final long windowStartTimestamp) {
    maybeTransitionToActive("put");
    observedStreamTime = Math.max(observedStreamTime, windowStartTimestamp);

    final Stamped<Bytes> wKey = new Stamped<>(key, windowStartTimestamp);

    buffer.put(wKey, value);
    StoreQueryUtils.updatePosition(position, context);
  }

  @Override
  public byte[] fetch(final Bytes key, final long time) {
    maybeTransitionToActive("fetch");

    final Result<Stamped<Bytes>> localResult = buffer.get(new Stamped<>(key, time));
    if (localResult != null)  {
      return localResult.isTombstone ? null : localResult.value;
    }

    final int partition = partitioner.partition(topicPartition.partition(), key);
    try (final KeyValueIterator<Stamped<Bytes>, byte[]> remoteResult = schema.fetch(
        name.cassandraName(),
        partition,
        key,
        time,
        time + 1
    )) {

      if (!remoteResult.hasNext()) {
        return null;
      }

      return remoteResult.next().value;
    }
  }

  @Override
  public WindowStoreIterator<byte[]> fetch(
      final Bytes key,
      final long timeFrom,
      final long timeTo
  ) {
    maybeTransitionToActive("fetch");

    final long start = Math.max(observedStreamTime - retentionPeriod, timeFrom);
    final Stamped<Bytes> from = new Stamped<>(key, start);
    final Stamped<Bytes> to = new Stamped<>(key, timeTo);

    final int subPartition = partitioner.partition(topicPartition.partition(), key);
    return Iterators.windowed(
        new LocalRemoteKvIterator<>(
            buffer.range(from, to),
            schema.fetch(name.cassandraName(), subPartition, key, start, timeTo),
            ResponsiveWindowStore::compareKeys
        )
    );
  }

  @Override
  public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(
      final Bytes keyFrom,
      final Bytes keyTo,
      final long timeFrom,
      final long timeTo
  ) {
    maybeTransitionToActive("fetch");
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  @Override
  public WindowStoreIterator<byte[]> backwardFetch(
      final Bytes key,
      final long timeFrom,
      final long timeTo
  ) {
    maybeTransitionToActive("backwardFetch");

    final long start = Math.max(observedStreamTime - retentionPeriod, timeFrom);
    final Stamped<Bytes> from = new Stamped<>(key, start);
    final Stamped<Bytes> to = new Stamped<>(key, timeTo);

    final int subPartition = partitioner.partition(topicPartition.partition(), key);
    return Iterators.windowed(
        new LocalRemoteKvIterator<>(
            buffer.backRange(from, to),
            schema.backFetch(name.cassandraName(), subPartition, key, start, timeTo),
            ResponsiveWindowStore::compareKeys
        )
    );
  }

  @Override
  public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetch(
      final Bytes keyFrom,
      final Bytes keyTo,
      final long timeFrom,
      final long timeTo
  ) {
    maybeTransitionToActive("backwardFetch");
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public KeyValueIterator<Windowed<Bytes>, byte[]> fetchAll(
      final long timeFrom,
      final long timeTo
  ) {
    maybeTransitionToActive("fetchAll");
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetchAll(
      final long timeFrom,
      final long timeTo
  ) {
    maybeTransitionToActive("backwardFetchAll");
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public KeyValueIterator<Windowed<Bytes>, byte[]> all() {
    maybeTransitionToActive("all");
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public KeyValueIterator<Windowed<Bytes>, byte[]> backwardAll() {
    maybeTransitionToActive("backwardAll");
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public void flush() {
  }

  @Override
  public void close() {
    if (storeRegistry != null) {
      storeRegistry.deregisterStore(registration);
    }
    // the client is shared across state stores, so only the
    // buffer needs to be flushed
    flush();
    open = false;
  }

  @Override
  public Position getPosition() {
    return position;
  }

  private boolean withinRetention(final Stamped<Bytes> key) {
    return key.stamp > observedStreamTime - retentionPeriod;
  }

  public static int compareKeys(final Stamped<Bytes> o1, final Stamped<Bytes> o2) {
    final int key = o1.key.compareTo(o2.key);
    if (key != 0) {
      return key;
    }

    return Long.compare(o1.stamp, o2.stamp);
  }

}
