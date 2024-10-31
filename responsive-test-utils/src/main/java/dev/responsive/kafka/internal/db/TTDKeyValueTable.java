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

package dev.responsive.kafka.internal.db;

import dev.responsive.kafka.internal.clients.TTDCassandraClient;
import dev.responsive.kafka.internal.db.inmemory.InMemoryKVTable;
import dev.responsive.kafka.internal.db.spec.RemoteTableSpec;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueIterator;

public class TTDKeyValueTable extends InMemoryKVTable {

  private final TTDCassandraClient client;

  public TTDKeyValueTable(final RemoteTableSpec spec, final TTDCassandraClient client) {
    super(spec.tableName(), spec.ttlResolver());
    this.client = client;
  }

  @Override
  public byte[] get(final int kafkaPartition, final Bytes key, final long streamTimeMs) {
    // trigger flush before lookup since CommitBuffer doesn't apply ttl
    client.flush();

    // take the max of mock "wallclock" time and stream-time since ttl is applied on both
    final long currentTimeMs = Math.max(streamTimeMs, client.currentWallClockTimeMs());

    return super.get(kafkaPartition, key, currentTimeMs);
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> range(
      final int kafkaPartition,
      final Bytes from,
      final Bytes to,
      final long streamTimeMs
  ) {
    // trigger flush before lookup since CommitBuffer doesn't apply ttl
    client.flush();

    // take the max of mock "wallclock" time and stream-time since ttl is applied on both
    final long currentTimeMs = Math.max(streamTimeMs, client.currentWallClockTimeMs());

    return super.range(kafkaPartition, from, to, currentTimeMs);
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> all(final int kafkaPartition, final long streamTimeMs) {
    // trigger flush before lookup since CommitBuffer doesn't apply ttl
    client.flush();

    // take the max of mock "wallclock" time and stream-time since ttl is applied on both
    final long currentTimeMs = Math.max(streamTimeMs, client.currentWallClockTimeMs());

    return super.all(kafkaPartition, currentTimeMs);
  }

  @Override
  public long approximateNumEntries(final int kafkaPartition) {
    return client.count(name(), kafkaPartition);
  }
}
