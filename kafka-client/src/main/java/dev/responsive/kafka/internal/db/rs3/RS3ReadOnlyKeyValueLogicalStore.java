package dev.responsive.kafka.internal.db.rs3;

import dev.responsive.kafka.internal.db.rs3.client.PssCheckpoint;
import dev.responsive.kafka.internal.db.rs3.client.jni.JNIRs3ReaderClient;
import dev.responsive.kafka.internal.snapshot.Snapshot;
import dev.responsive.kafka.internal.utils.MergeKeyValueIterator;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.clients.producer.internals.BuiltInPartitioner;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueIterator;

// todo: yucky name
// A view over the whole keyspace of a key value store. We use a concrete type
// rather than a generic because we want specialized scan behaviour for different
// store types. For example, a window store could restrict range scans for windows
// of a single key to one partition.
public class RS3ReadOnlyKeyValueLogicalStore<K, V> {
  private final List<? extends RS3ReadOnlyKeyValueStore<Bytes, V>> stores;
  private final Function<Bytes, Integer> partitioner;
  private final Serde<K> keySerde;

  public RS3ReadOnlyKeyValueLogicalStore(
      final List<? extends RS3ReadOnlyKeyValueStore<Bytes, V>> stores,
      final Function<Bytes, Integer> partitioner,
      final Serde<K> keySerde
  ) {
    this.stores = stores;
    this.partitioner = partitioner;
    this.keySerde = keySerde;
  }

  public V get(K key) {
    final Bytes serializedKey = serializeKey(key);
    final int partition = partitioner.apply(serializedKey);
    return stores.get(partition).get(serializedKey);
  }

  public KeyValueIterator<K, V> range(K from, K to) {
    final Bytes serializedFrom = serializeKey(from);
    final Bytes serializedTo = serializeKey(to);
    return iterate(s -> s.range(serializedFrom, serializedTo));
  }

  public KeyValueIterator<K, V> all() {
    return iterate(RS3ReadOnlyKeyValueStore::all);
  }

  private KeyValueIterator<K, V> iterate(
      final Function<RS3ReadOnlyKeyValueStore<Bytes, V>, KeyValueIterator<Bytes, V>> iteratorFn) {
    final List<KeyValueIterator<Bytes, V>> iterators = new ArrayList<>(stores.size());
    try {
      for (final RS3ReadOnlyKeyValueStore<Bytes, V> store : stores) {
        iterators.add(iteratorFn.apply(store));
      }
      final var mergingIterator = new MergeKeyValueIterator<>(iterators);
      return new TransformingIterator<>(
          mergingIterator,
          this::deserializeKey,
          v -> v
      );
    } catch (RuntimeException e) {
      for (final var i : iterators) {
        i.close();
      }
      throw e;
    }
  }

  private Bytes serializeKey(final K key) {
    return Bytes.wrap(keySerde.serializer().serialize("", key));
  }

  private K deserializeKey(final Bytes key) {
    return keySerde.deserializer().deserialize("", key.get());
  }

  public static <K extends Comparable<K>, V> RS3ReadOnlyKeyValueLogicalStore<K, V>
  createFromSnapshot(
      final JNIRs3ReaderClient readerClient,
      final Snapshot snapshot,
      final String storeName,
      final Serde<K> keySerde,
      final Serde<V> valueSerde
  ) {
    return createFromSnapshot(
        readerClient,
        snapshot,
        storeName,
        keySerde,
        valueSerde,
        (k, p) -> BuiltInPartitioner.partitionForKey(k.get(), p)
    );
  }

  public static <K extends Comparable<K>, V> RS3ReadOnlyKeyValueLogicalStore<K, V>
  createFromSnapshot(
      final JNIRs3ReaderClient readerClient,
      final Snapshot snapshot,
      final String storeName,
      final Serde<K> keySerde,
      final Serde<V> valueSerde,
      final BiFunction<Bytes, Integer, Integer> partitioner
  ) {
    final var checkpointsByPartition = snapshot.taskSnapshots()
        .stream()
        .filter(ts -> ts.checkpoints().containsKey(storeName))
        .collect(Collectors.toMap(
            Snapshot.TaskSnapshotMetadata::taskId,
            ts -> TableCheckpoint.deserialize(ts.checkpoints().get(storeName))
        ));
    final UUID storeReaderId = UUID.randomUUID();
    int npartitions = 0;
    int maxPartition = 0;
    Integer subtopology = null;
    for (final var tid : checkpointsByPartition.keySet()) {
      if (subtopology != null && subtopology != tid.subtopology()) {
        throw new IllegalStateException("found multiple subtopologies for store " + storeName);
      }
      subtopology = tid.subtopology();
      maxPartition = Math.max(maxPartition, tid.partition());
      npartitions++;
    }
    if (npartitions != maxPartition + 1) {
      throw new IllegalStateException("missing partitions for " + storeName);
    }
    final Map<Integer, PssCheckpoint> pssCheckpointsByPss = new HashMap<>();
    for (final TableCheckpoint cp : checkpointsByPartition.values()) {
      for (final TableCheckpoint.TablePssCheckpoint tcp: cp.pssCheckpoints()) {
        if (pssCheckpointsByPss.containsKey(tcp.checkpoint.pssId())) {
          if (!pssCheckpointsByPss.get(tcp.checkpoint.pssId()).equals(tcp.checkpoint())) {
            throw new IllegalStateException("found duplicate pss checkpoint for store " + storeName);
          }
          continue;
        }
        pssCheckpointsByPss.put(tcp.checkpoint.pssId(), tcp.checkpoint());
        readerClient.loadPssReaderFromCheckpoint(
            storeReaderId,
            tcp.checkpoint()
        );
      }
    }
    final ArrayList<RS3ReadOnlyKeyValueStore<Bytes, V>> stores = new ArrayList<>();
    for (int i = 0; i < npartitions; i++) {
      final var bytesStore = new RS3ReadOnlyKeyValueBytesStore(readerClient, storeReaderId, i);
      stores.add(RS3ReadOnlyDeserializingKeyValueStore.forTimestamped(
          bytesStore,
          Serdes.Bytes(),
          valueSerde
      ));
    }
    final int finalPartitions = npartitions;
    return new RS3ReadOnlyKeyValueLogicalStore<>(
        stores,
        k -> partitioner.apply(k, finalPartitions),
        keySerde
    );
  }
}
