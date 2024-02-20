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

package dev.responsive.kafka.internal.db;

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import dev.responsive.kafka.internal.clients.TTDCassandraClient;
import dev.responsive.kafka.internal.db.partitioning.Segmenter.SegmentPartition;
import dev.responsive.kafka.internal.db.partitioning.TablePartitioner;
import dev.responsive.kafka.internal.db.partitioning.WindowSegmentPartitioner;
import dev.responsive.kafka.internal.db.spec.CassandraTableSpec;
import dev.responsive.kafka.internal.stores.RemoteWriteResult;
import dev.responsive.kafka.internal.stores.WindowStoreStub;
import dev.responsive.kafka.internal.utils.WindowedKey;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueIterator;

public class TTDWindowedTable extends TTDTable<WindowedKey>
    implements RemoteWindowedTable<BoundStatement>  {

  private final String name;
  private final WindowStoreStub stub;
  private final WindowSegmentPartitioner partitioner;

  public static TTDWindowedTable create(
      final CassandraTableSpec spec,
      final CassandraClient client,
      final WindowSegmentPartitioner partitioner
  ) {
    return new TTDWindowedTable(spec, (TTDCassandraClient) client, partitioner);
  }

  public TTDWindowedTable(
      final CassandraTableSpec spec,
      final TTDCassandraClient client,
      WindowSegmentPartitioner partitioner
  ) {
    super(client);
    this.name = spec.tableName();
    this.stub = new WindowStoreStub();
    this.partitioner = partitioner;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public WindowFlushManager init(final int kafkaPartition) {
    return new TTDWindowFlushManager(this, kafkaPartition, partitioner);
  }

  @Override
  public BoundStatement insert(
      final int kafkaPartition,
      final WindowedKey key,
      final byte[] value,
      final long epochMillis
  ) {
    stub.put(key, value);
    return null;
  }

  @Override
  public BoundStatement delete(
      final int kafkaPartition,
      final WindowedKey key
  ) {
    stub.delete(key);
    return null;
  }

  @Override
  public byte[] fetch(
      int kafkaPartition,
      Bytes key,
      long windowStart
  ) {
    return stub.fetch(key, windowStart);
  }

  @Override
  public KeyValueIterator<WindowedKey, byte[]> fetch(
      final int kafkaPartition,
      final Bytes key,
      final long timeFrom,
      final long timeTo
  ) {
    return stub.fetch(key, timeFrom, timeTo);
  }

  @Override
  public KeyValueIterator<WindowedKey, byte[]> backFetch(
      final int kafkaPartition,
      final Bytes key,
      final long timeFrom,
      final long timeTo
  ) {
    return stub.backFetch(key, timeFrom, timeTo);
  }

  @Override
  public KeyValueIterator<WindowedKey, byte[]> fetchRange(
      final int kafkaPartition,
      final Bytes fromKey,
      final Bytes toKey,
      final long timeFrom,
      final long timeTo
  ) {
    return stub.fetchRange(fromKey, toKey, timeFrom, timeTo);
  }

  @Override
  public KeyValueIterator<WindowedKey, byte[]> backFetchRange(
      final int kafkaPartition,
      final Bytes fromKey,
      final Bytes toKey,
      final long timeFrom,
      final long timeTo
  ) {
    return stub.backFetchRange(fromKey, toKey, timeFrom, timeTo);
  }

  @Override
  public KeyValueIterator<WindowedKey, byte[]> fetchAll(
      final int kafkaPartition,
      final long timeFrom,
      final long timeTo
  ) {
    return stub.fetchAll(timeFrom, timeTo);
  }

  @Override
  public KeyValueIterator<WindowedKey, byte[]> backFetchAll(
      final int kafkaPartition,
      final long timeFrom,
      final long timeTo
  ) {
    return stub.backFetchAll(timeFrom, timeTo);
  }

  @Override
  public long count() {
    return 0;
  }

  private static class TTDWindowFlushManager extends WindowFlushManager {

    private final String logPrefix;
    private final TTDWindowedTable table;
    private final WindowSegmentPartitioner partitioner;

    public TTDWindowFlushManager(
        final TTDWindowedTable table,
        final int kafkaPartition,
        final WindowSegmentPartitioner partitioner
    ) {
      super(table.name(), kafkaPartition, partitioner.segmenter, 0L);
      this.table = table;
      this.partitioner = partitioner;
      this.logPrefix = String.format("%s TTDWindowFlushManager ", table.name());
    }

    @Override
    public String tableName() {
      return table.name();
    }

    @Override
    public TablePartitioner<WindowedKey, SegmentPartition> partitioner() {
      return partitioner;
    }

    @Override
    public RemoteWriter<WindowedKey, SegmentPartition> createWriter(
        final SegmentPartition tablePartition
    ) {
      return new TTDWriter<>(table, tablePartition);
    }

    @Override
    public String failedFlushInfo(
        final long batchOffset,
        final SegmentPartition failedTablePartition
    ) {
      return "";
    }

    @Override
    public String logPrefix() {
      return logPrefix;
    }

    @Override
    protected RemoteWriteResult<SegmentPartition> updateOffsetAndStreamTime(
        final long consumedOffset,
        final long streamTime
    ) {
      return null;
    }

    @Override
    protected RemoteWriteResult<SegmentPartition> createSegment(final SegmentPartition partition) {
      return null;
    }

    @Override
    protected RemoteWriteResult<SegmentPartition> deleteSegment(final SegmentPartition partition) {
      return null;
    }
  }
}
