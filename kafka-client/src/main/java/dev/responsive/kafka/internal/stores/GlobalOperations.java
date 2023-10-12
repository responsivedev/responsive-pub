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

import dev.responsive.kafka.api.stores.ResponsiveKeyValueParams;
import dev.responsive.kafka.internal.db.CassandraClient;
import dev.responsive.kafka.internal.db.CassandraTableSpecFactory;
import dev.responsive.kafka.internal.db.RemoteKVTable;
import dev.responsive.kafka.internal.db.partitioning.SubPartitioner;
import dev.responsive.kafka.internal.utils.SharedClients;
import java.util.Collection;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.GlobalProcessorContextImpl;
import org.apache.kafka.streams.state.KeyValueIterator;

public class GlobalOperations implements KeyValueOperations {

  private static final long ALL_VALID_TS = -1L; // Global stores don't support TTL
  private static final int IGNORED_PARTITION = -1; // Global stores ignored partitions

  private final GlobalProcessorContextImpl context;
  private final CassandraClient client;
  private final RemoteKVTable table;

  public static GlobalOperations create(
      final StateStoreContext storeContext,
      final ResponsiveKeyValueParams params
  ) throws InterruptedException, TimeoutException {
    final var context = (GlobalProcessorContextImpl) storeContext;
    final SharedClients sharedClients = SharedClients.loadSharedClients(context.appConfigs());
    final var client = sharedClients.cassandraClient();
    final var spec = CassandraTableSpecFactory.globalSpec(params);
    final var table = client.globalFactory().create(spec);

    table.init(SubPartitioner.NO_SUBPARTITIONS, IGNORED_PARTITION);
    return new GlobalOperations(context, client, table);
  }

  public GlobalOperations(
      final GlobalProcessorContextImpl context,
      final CassandraClient client,
      final RemoteKVTable table
  ) {
    this.context = context;
    this.client = client;
    this.table = table;
  }

  @Override
  public void register(final ResponsiveStoreRegistry storeRegistry) {
    // we don't do anything with global tables
  }

  @Override
  public void deregister(final ResponsiveStoreRegistry storeRegistry) {
    // we don't do anything with global tables
  }

  @Override
  public void put(final Bytes key, final byte[] value) {
    put(key, value, context.partition(), context.offset(), context.timestamp());
  }

  private void put(
      final Bytes key,
      final byte[] value,
      final int partition,
      final long offset,
      final long timestamp
  ) {
    client.execute(table.insert(IGNORED_PARTITION, key, value, timestamp));
    client.execute(table.setOffset(partition, offset));
  }

  @Override
  public byte[] delete(final Bytes key) {
    return delete(key, context.partition(), context.offset());
  }

  private byte[] delete(final Bytes key, final int partition, final long offset) {
    final byte[] bytes = get(key);
    client.execute(table.delete(IGNORED_PARTITION, key));
    client.execute(table.setOffset(partition, offset));
    return bytes;
  }

  @Override
  public byte[] get(final Bytes key) {
    return table.get(IGNORED_PARTITION, key, ALL_VALID_TS);
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> range(final Bytes from, final Bytes to) {
    return table.range(IGNORED_PARTITION, from, to, ALL_VALID_TS);
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> reverseRange(final Bytes from, final Bytes to) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> all() {
    return table.all(IGNORED_PARTITION, ALL_VALID_TS);
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> reverseAll() {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public long approximateNumEntries() {
    return client.count(table.name(), IGNORED_PARTITION);
  }

  @Override
  public void close() {
  }

  @Override
  public void restoreBatch(final Collection<ConsumerRecord<byte[], byte[]>> records) {
    for (final var rec : records) {
      // We should consider using the same strategy we use in the
      // ResponsiveRestoreConsumer and just seek to where we need to
      // start from instead of skipping records - this is just less wiring
      // code for now and the number of records to skip is pretty minimal
      // (limited by the number of records committed between a single window
      // of auto.commit.interval.ms) unlike the changelog equivalent which
      // always restores from scratch
      final int partition = rec.partition();
      final long offset = table.metadata(partition).offset;
      if (rec.offset() < offset) {
        // ignore records that have already been processed - race conditions
        // are not important since the worst case we'll have just not written
        // the last record, which we just re-process here (we update the offset
        // after each write to remote)
        continue;
      }

      if (rec.value() == null) {
        delete(new Bytes(rec.key()), rec.partition(), rec.offset());
      } else {
        put(new Bytes(rec.key()), rec.value(), rec.partition(), rec.offset(), rec.timestamp());
      }
    }
  }

}
