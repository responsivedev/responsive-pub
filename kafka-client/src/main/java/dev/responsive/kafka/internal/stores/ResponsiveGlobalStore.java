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

import static dev.responsive.kafka.internal.utils.SharedClients.loadSharedClients;

import dev.responsive.kafka.api.stores.ResponsiveKeyValueParams;
import dev.responsive.kafka.internal.config.InternalConfigs;
import dev.responsive.kafka.internal.db.RemoteKeyValueSchema;
import dev.responsive.kafka.internal.db.partitioning.SubPartitioner;
import dev.responsive.kafka.internal.utils.SharedClients;
import java.util.List;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.GlobalProcessorContextImpl;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.internals.StoreQueryUtils;
import org.slf4j.Logger;

/**
 * The {@code ResponsiveGlobalStore}
 */
public class ResponsiveGlobalStore extends AbstractResponsiveStore
    implements KeyValueStore<Bytes, byte[]> {

  private static final long ALL_VALID_TS = -1L; // Global stores don't support TTL

  private final Logger log;
  private final ResponsiveKeyValueParams params;
  private RemoteKeyValueSchema schema;

  public ResponsiveGlobalStore(final ResponsiveKeyValueParams params) {
    super(params.name());
    this.params = params;
    log = new LogContext(
        String.format("global-store [%s]", name.kafkaName())
    ).logger(ResponsiveGlobalStore.class);
    throw new UnsupportedOperationException("Global tables are not available for use at this time");
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
      log.info("Initializing global state store {}", name);

      final SharedClients sharedClients = loadSharedClients(storeContext.appConfigs());

      // this is bad, but the assumption is that global tables are small
      // and can fit in a single topicPartition - all writers will write using
      // the same topicPartition key. we should consider using ALLOW FILTERING
      // instead. that would make gets/puts more efficient and the queries
      // to be more evently distributed and take the hit only on range queries
      final TopicPartition globalTopicPartition =
          new TopicPartition(((GlobalProcessorContextImpl) storeContext).topic(), 0);

      super.init(
          storeContext,
          sharedClients.cassandraClient,
          InternalConfigs.loadStoreRegistry(storeContext.appConfigs()),
          globalTopicPartition
      );

      schema = sharedClients.cassandraClient.prepareKVTableSchema(params);

      schema.init(
          name.cassandraName(),
          SubPartitioner.NO_SUBPARTITIONS,
          changelog.partition()
      );
      log.info("Global table {} is available for querying.", name);

      super.open(root, batch -> {
        for (final ConsumerRecord<byte[], byte[]> record : batch) {
          put(Bytes.wrap(record.key()), record.value());
        }
      });

    } catch (InterruptedException | TimeoutException e) {
      throw new ProcessorStateException("Failed to initialize store.", e);
    }
  }

  @Override
  public void put(final Bytes key, final byte[] value) {
    if (value == null) {
      delete(key);
    } else {
      putInternal(key, value);
    }
  }

  @Override
  public byte[] putIfAbsent(final Bytes key, final byte[] value) {
    final byte[] old = get(key);
    if (old == null && value != null) {
      putInternal(key, value);
    }
    return old;
  }

  @Override
  public void putAll(final List<KeyValue<Bytes, byte[]>> entries) {
    entries.forEach(kv -> put(kv.key, kv.value));
  }

  private void putInternal(final Bytes key, final byte[] value) {
    client.execute(schema.insert(
        name.cassandraName(),
        changelog.partition(),
        key,
        value,
        context.timestamp())
    );
    StoreQueryUtils.updatePosition(position, context);
  }

  @Override
  public byte[] delete(final Bytes key) {
    final byte[] bytes = get(key);
    client.execute(schema.delete(name.cassandraName(), changelog.partition(), key));
    StoreQueryUtils.updatePosition(position, context);
    return bytes;
  }

  @Override
  public byte[] get(final Bytes key) {
    return schema.get(name.cassandraName(), changelog.partition(), key, ALL_VALID_TS);
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> range(final Bytes from, final Bytes to) {
    return schema.range(name.cassandraName(), changelog.partition(), from, to, ALL_VALID_TS);
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> all() {
    return schema.all(name.cassandraName(), changelog.partition(), ALL_VALID_TS);
  }

  @Override
  public long approximateNumEntries() {
    return client.count(name.cassandraName(), changelog.partition());
  }

  @Override
  public void close() {
   // nothing to do for the global store since we don't register it or go through a commit buffer
  }

}
