package dev.responsive.kafka.api.async;

import com.google.common.collect.ImmutableList;
import dev.responsive.kafka.api.async.AsyncProcessor.Finalizer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.state.KeyValueStore;

// TODO: schedule punctuation to poke when there is no traffic

public class ResponsiveAsyncProcessor<KIn, VIn, KOut, VOut>
    implements Processor<KIn, VIn, KOut, VOut> {
  private static final String KEY_SUFFIX = ".unflushed";
  private final AsyncProcessor<KIn, VIn, KOut, VOut> wrapped;
  private final int maxUnflushed;
  private Map<String, AsyncKeyValueStore<?, ?>> asyncStores;
  private Map<String, StateStore> stores;
  private ProcessorContext<KOut, VOut> context;
  private KeyValueStore<String, UnflushedRecords<KIn, VIn>> unflushedStore;
  private final Map<Long, Future<Finalizer<KOut, VOut>>> finalizers = new HashMap<>();
  private String unflushedKey;

  public ResponsiveAsyncProcessor(
      final AsyncProcessor<KIn, VIn, KOut, VOut> wrapped,
      final int maxUnflushed) {
    this.wrapped = Objects.requireNonNull(wrapped);
    this.maxUnflushed = maxUnflushed;
  }

  @Override
  public void init(final ProcessorContext<KOut, VOut> context) {
    this.context = context;
    unflushedStore = context.getStateStore("async-processor");
    unflushedKey = context.taskId() + KEY_SUFFIX;
    this.stores = wrapped.stores().stream().collect(Collectors.toMap(
        s -> s,
        context::getStateStore
    ));
    this.asyncStores = stores.entrySet().stream()
        .filter(e -> e.getValue() instanceof AsyncKeyValueStore)
        .collect(Collectors.toMap(Entry::getKey, e -> (AsyncKeyValueStore<?, ?>) e.getValue()));
  }

  private void pokeUnflushed(final UnflushedRecords<KIn, VIn> unflushed, boolean block) {
    final Set<KIn> unflushedKeys = new HashSet<>();
    for (final UnflushedRecord<KIn, VIn> unflushedRecord : unflushed.records()) {
      if (unflushedKeys.contains(unflushedRecord.key())) {
        // we already have an outstanding process call for this key, wait for it to finish
        continue;
      }
      unflushedKeys.add(unflushedRecord.key());
      if (!finalizers.containsKey(unflushedRecord.offset())) {
        finalizers.put(
            unflushedRecord.offset,
            wrapped.processAsync(asyncStores, new Record<>(
                unflushedRecord.key(),
                unflushedRecord.value(),
                unflushedRecord.timestmap()
            ))
        );
      }
      final Future<Finalizer<KOut, VOut>> future = finalizers.get(unflushedRecord.offset());
      if (block || future.isDone()) {
        final Finalizer<KOut, VOut> finalizer;
        try {
          finalizer = future.get();
        } catch (final InterruptedException e) {
          throw new RuntimeException(e);
        } catch (final ExecutionException e) {
          throw new RuntimeException(e.getCause());
        }
        finalizer.maybeForward(stores, context);
        block = false;
        finalizers.remove(unflushedRecord.offset());
        unflushedKeys.remove(unflushedRecord.key());
      }
    }
  }

  @Override
  public void process(final Record<KIn, VIn> record) {
    final Optional<RecordMetadata> maybeMetadata = context.recordMetadata();
    if (maybeMetadata.isEmpty()) {
      throw new IllegalStateException("cannot use async processor with upstream punctuation");
    }
    final long offset = maybeMetadata.get().offset();
    UnflushedRecords<KIn, VIn> unflushed = unflushedStore.get(unflushedKey);
    if (unflushed == null) {
      unflushed = new UnflushedRecords<>(List.of());
    }
    final UnflushedRecords<KIn, VIn> withNext = new UnflushedRecords<>(
        ImmutableList.<UnflushedRecord<KIn, VIn>>builder()
            .addAll(unflushed.records())
            .add(new UnflushedRecord<>(
                offset,
                record.timestamp(),
                record.key(),
                record.value()))
            .build()
    );
    unflushedStore.put(unflushedKey, withNext);
    pokeUnflushed(withNext, withNext.records().size() >= maxUnflushed);
  }

  @Override
  public void close() {
    wrapped.close();
  }

  private static class UnflushedRecord<K, V> {
    private final long offset;
    private final long timestmap;
    private final K key;
    private final V value;

    private UnflushedRecord(final long offset, final long timestmap, final K key, final V value) {
      this.offset = offset;
      this.timestmap = timestmap;
      this.key = key;
      this.value = value;
    }

    private long offset() {
      return offset;
    }

    private long timestmap() {
      return timestmap;
    }

    private K key() {
      return key;
    }

    private V value() {
      return value;
    }
  }

  static final class UnflushedRecords<K, V> {
    private final List<UnflushedRecord<K, V>> records;

    private UnflushedRecords(final List<UnflushedRecord<K, V>> records) {
      this.records = Objects.requireNonNull(records, "records");
    }

    public List<UnflushedRecord<K, V>> records() {
      return records;
    }
  }
}
