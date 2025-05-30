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

package dev.responsive.kafka.internal.db.inmemory;

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import dev.responsive.kafka.api.stores.TtlProvider.TtlDuration;
import dev.responsive.kafka.internal.db.KVFlushManager;
import dev.responsive.kafka.internal.db.RemoteKVTable;
import dev.responsive.kafka.internal.db.RemoteWriter;
import dev.responsive.kafka.internal.db.partitioning.TablePartitioner;
import dev.responsive.kafka.internal.stores.RemoteWriteResult;
import dev.responsive.kafka.internal.stores.ResponsiveStoreRegistration;
import dev.responsive.kafka.internal.stores.TtlResolver;
import dev.responsive.kafka.internal.utils.Iterators;
import dev.responsive.kafka.internal.utils.Utils;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Note: this class doesn't actually use BoundStatements and applies all operations immediately,
// we just stub the BoundStatement type so we can reuse this table for the TTD
public class InMemoryKVTable implements RemoteKVTable<BoundStatement> {
  private static final Logger LOG = LoggerFactory.getLogger(InMemoryKVTable.class);

  private int kafkaPartition;
  private final String name;
  private final Optional<TtlResolver<?, ?>> ttlResolver;
  private final ConcurrentNavigableMap<Bytes, Value> store = new ConcurrentSkipListMap<>();

  public InMemoryKVTable(final String name, final Optional<TtlResolver<?, ?>> ttlResolver) {
    this.name = Objects.requireNonNull(name);
    this.ttlResolver = ttlResolver;
  }

  @Override
  public KVFlushManager init(int kafkaPartition) {
    LOG.info("init in-memory kv store {} {}", name, kafkaPartition);
    this.kafkaPartition = kafkaPartition;
    return new InMemoryKVFlushManager();
  }

  @Override
  public byte[] get(final int kafkaPartition, final Bytes key, final long streamTimeMs) {
    checkKafkaPartition(kafkaPartition);
    final var value = store.get(key);
    if (value == null) {
      return null;
    }

    if (ttlResolver.isPresent()) {
      final TtlDuration rowTtl = ttlResolver.get().resolveTtl(key, value.value());
      if (rowTtl.isFinite()) {
        final long minValidTs = streamTimeMs - rowTtl.toMillis();
        if (value.epochMillis < minValidTs) {
          return null;
        }
      }
    }

    return value.value();
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> range(
      final int kafkaPartition,
      final Bytes from,
      final Bytes to,
      final long streamTimeMs
  ) {
    return doRange(kafkaPartition, from, to, streamTimeMs, true);
  }

  @Override
  public <PS extends Serializer<P>, P> KeyValueIterator<Bytes, byte[]> prefix(
      final P prefix,
      final PS prefixKeySerializer,
      final int kafkaPartition,
      final long streamTimeMs
  ) {
    final Bytes from = Bytes.wrap(prefixKeySerializer.serialize(null, prefix));
    final Bytes to = Utils.incrementWithoutOverflow(from);

    return doRange(kafkaPartition, from, to, streamTimeMs, false);
  }

  private KeyValueIterator<Bytes, byte[]> doRange(
      final int kafkaPartition,
      final Bytes from,
      final Bytes to,
      final long streamTimeMs,
      final boolean endInclusive
  ) {
    if (ttlResolver.isPresent() && !ttlResolver.get().hasDefaultOnly()) {
      throw new UnsupportedOperationException("Row-level ttl is not yet supported for range "
          + "queries or prefix scans on in-memory tables or TTD");
    }

    checkKafkaPartition(kafkaPartition);

    var view = store;
    if (from != null) {
      view = view.tailMap(from, true);
    }
    if (to != null) {
      view = view.headMap(to, endInclusive);
    }

    final var iter = view.entrySet().iterator();

    final long minValidTs = ttlResolver.isEmpty()
        ? -1L
        : streamTimeMs - ttlResolver.get().defaultTtl().toMillis();

    return iteratorWithTimeFilter(iter, minValidTs);
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> all(final int kafkaPartition, final long streamTimeMs) {
    if (ttlResolver.isPresent() && !ttlResolver.get().hasDefaultOnly()) {
      throw new UnsupportedOperationException("Row-level ttl is not yet supported for range "
                                                  + "queries on in-memory tables or TTD");
    }

    checkKafkaPartition(kafkaPartition);
    final var iter = store.entrySet().iterator();

    final long minValidTs = ttlResolver.isEmpty()
        ? -1L
        : streamTimeMs - ttlResolver.get().defaultTtl().toMillis();

    return iteratorWithTimeFilter(iter, minValidTs);
  }

  @Override
  public long approximateNumEntries(int kafkaPartition) {
    checkKafkaPartition(kafkaPartition);
    return store.size();
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public BoundStatement insert(int kafkaPartition, Bytes key, byte[] value, long timestampMs) {
    checkKafkaPartition(kafkaPartition);

    store.put(key, new Value(timestampMs, value));
    return null;
  }

  @Override
  public BoundStatement delete(int kafkaPartition, Bytes key) {
    checkKafkaPartition(kafkaPartition);
    store.remove(key);

    return null;
  }

  @Override
  public long lastWrittenOffset(int kafkaPartition) {
    checkKafkaPartition(kafkaPartition);
    return ResponsiveStoreRegistration.NO_COMMITTED_OFFSET;
  }

  private KeyValueIterator<Bytes, byte[]> iteratorWithTimeFilter(
      final Iterator<Map.Entry<Bytes, Value>> iter,
      final long minValidTs
  ) {
    return Iterators.kv(
        Iterators.filter(
            Iterators.kv(iter, e -> new KeyValue<>(e.getKey(), e.getValue())),
            kv -> kv.value.epochMillis >= minValidTs
        ),
        kv -> new KeyValue<>(kv.key, kv.value.value())
    );
  }

  public class InMemoryKVFlushManager extends KVFlushManager {
    private InMemoryKVFlushManager() {
    }

    @Override
    public RemoteWriteResult<Integer> updateOffset(long consumedOffset) {
      return RemoteWriteResult.success(kafkaPartition);
    }

    @Override
    public String tableName() {
      return name;
    }

    @Override
    public TablePartitioner<Bytes, Integer> partitioner() {
      return TablePartitioner.defaultPartitioner();
    }

    @Override
    public RemoteWriter<Bytes, Integer> createWriter(
        final Integer tablePartition,
        final long consumedOffset
    ) {
      return new RemoteWriter<>() {
        @Override
        public void insert(Bytes key, byte[] value, long timestampMs) {
          InMemoryKVTable.this.insert(tablePartition, key, value, timestampMs);
        }

        @Override
        public void delete(Bytes key) {
          InMemoryKVTable.this.delete(tablePartition, key);
        }

        @Override
        public CompletionStage<RemoteWriteResult<Integer>> flush() {
          return CompletableFuture.completedFuture(RemoteWriteResult.success(kafkaPartition));
        }
      };
    }

    @Override
    public String failedFlushInfo(long batchOffset, Integer failedTablePartition) {
      return "failed flush";
    }

    @Override
    public String logPrefix() {
      return "inmemory";
    }
  }

  private void checkKafkaPartition(int kafkaPartition) {
    if (this.kafkaPartition != kafkaPartition) {
      throw new IllegalStateException(
          "unexpected partition: " + kafkaPartition + " for " + this.kafkaPartition);
    }
  }

  private static class Value {
    private final long epochMillis;
    private final byte[] value;

    public Value(final long epochMillis, final byte[] value) {
      this.epochMillis = epochMillis;
      this.value = value;
    }

    public long epochMillis() {
      return epochMillis;
    }

    public byte[] value() {
      return value;
    }
  }
}
