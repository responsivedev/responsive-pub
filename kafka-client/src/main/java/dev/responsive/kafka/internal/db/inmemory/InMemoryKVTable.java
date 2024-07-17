package dev.responsive.kafka.internal.db.inmemory;

import dev.responsive.kafka.internal.db.KVFlushManager;
import dev.responsive.kafka.internal.db.RemoteKVTable;
import dev.responsive.kafka.internal.db.RemoteWriter;
import dev.responsive.kafka.internal.db.partitioning.TablePartitioner;
import dev.responsive.kafka.internal.stores.RemoteWriteResult;
import dev.responsive.kafka.internal.stores.ResponsiveStoreRegistration;
import dev.responsive.kafka.internal.utils.Iterators;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InMemoryKVTable implements RemoteKVTable<Object> {
  private static final Logger LOG = LoggerFactory.getLogger(InMemoryKVTable.class);

  private int kafkaPartition;
  private final String name;
  private final ConcurrentNavigableMap<Bytes, Value> store = new ConcurrentSkipListMap<>();

  public InMemoryKVTable(String name) {
    this.name = Objects.requireNonNull(name);
  }

  @Override
  public KVFlushManager init(int kafkaPartition) {
    LOG.info("init in-memory kv store {} {}", name, kafkaPartition);
    this.kafkaPartition = kafkaPartition;
    return new InMemoryKVFlushManager();
  }

  @Override
  public byte[] get(final int kafkaPartition, final Bytes key, final long minValidTs) {
    checkKafkaPartition(kafkaPartition);
    final var value = store.get(key);
    if (value == null) {
      return null;
    }
    if (value.epochMillis() < minValidTs) {
      return null;
    }
    return value.value();
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> range(
      final int kafkaPartition,
      final Bytes from,
      final Bytes to,
      final long minValidTs
  ) {
    checkKafkaPartition(kafkaPartition);
    final var iter = store
        .tailMap(from, true)
        .headMap(to, true)
        .entrySet()
        .iterator();
    return iteratorWithTimeFilter(iter, minValidTs);
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> all(final int kafkaPartition, final long minValidTs) {
    checkKafkaPartition(kafkaPartition);
    final var iter = store.entrySet().iterator();
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
  public Object insert(int kafkaPartition, Bytes key, byte[] value, long epochMillis) {
    checkKafkaPartition(kafkaPartition);
    return store.put(key, new Value(epochMillis, value));
  }

  @Override
  public Object delete(int kafkaPartition, Bytes key) {
    checkKafkaPartition(kafkaPartition);
    return store.remove(key);
  }

  @Override
  public long fetchOffset(int kafkaPartition) {
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
      return new TablePartitioner<>() {
        @Override
        public Integer tablePartition(int kafkaPartition, Bytes key) {
          return kafkaPartition;
        }

        @Override
        public Integer metadataTablePartition(int kafkaPartition) {
          return kafkaPartition;
        }
      };
    }

    @Override
    public RemoteWriter<Bytes, Integer> createWriter(Integer tablePartition) {
      return new RemoteWriter<>() {
        @Override
        public void insert(Bytes key, byte[] value, long epochMillis) {
          InMemoryKVTable.this.insert(tablePartition, key, value, epochMillis);
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
