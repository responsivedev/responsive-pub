package dev.responsive.kafka.api.async;

import com.google.common.collect.ImmutableList;
import dev.responsive.kafka.api.async.AsyncProcessor.Finalizer;
import dev.responsive.kafka.api.async.UnflushedRecords.UnflushedRecord;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.internals.WrappedStateStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: schedule punctuation to poke when there is no traffic

public class ResponsiveAsyncProcessor<KIn, VIn, KOut, VOut>
    implements Processor<KIn, VIn, KOut, VOut> {
  private static final Logger LOG = LoggerFactory.getLogger(ResponsiveAsyncProcessor.class);
  private static final String KEY_SUFFIX = ".unflushed";
  private final AsyncProcessor<KIn, VIn, KOut, VOut> wrapped;
  private final int maxUnflushed;
  private final Serde<KIn> keySerde;
  private final Serde<VIn> valSerde;
  private Map<String, AsyncKeyValueStore<?, ?>> asyncStores;
  private Map<String, StateStore> stores;
  private ProcessorContext<KOut, VOut> context;
  private KeyValueStore<String, UnflushedRecords> unflushedStore;
  private final Map<Long, Future<Finalizer<KOut, VOut>>> finalizers = new HashMap<>();
  private String unflushedKey;

  public ResponsiveAsyncProcessor(
      final AsyncProcessor<KIn, VIn, KOut, VOut> wrapped,
      final int maxUnflushed, final Serde<KIn> keySerde, final Serde<VIn> valSerde) {
    this.wrapped = Objects.requireNonNull(wrapped);
    this.maxUnflushed = maxUnflushed;
    this.keySerde = Objects.requireNonNull(keySerde);
    this.valSerde = Objects.requireNonNull(valSerde);
  }

  private Optional<AsyncKeyValueStore<?, ?>> maybeLoadStore(final StateStore store) {
    if (store instanceof AsyncKeyValueStore) {
      return Optional.of((AsyncKeyValueStore<?, ?>) store);
    }
    try {
      //final Method m = store.getClass().getDeclaredMethod("wrapped");
      final Method m = WrappedStateStore.class.getDeclaredMethod("wrapped");
      final Object wrapped = m.invoke(store);
      if (wrapped instanceof AsyncKeyValueStore) {
        return Optional.of((AsyncKeyValueStore<?, ?>) wrapped);
      }
    } catch (final NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
      LOG.info("error calling wrapped", e);
    }
    LOG.info("unknown store type " + store.getClass().getName());
    return Optional.empty();
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
        .map(e -> Map.entry(e.getKey(), maybeLoadStore(e.getValue())))
        .filter(e -> e.getValue().isPresent())
        .collect(Collectors.toMap(Entry::getKey, e -> e.getValue().get()));
  }

  private void pokeUnflushed(final UnflushedRecords unflushed, boolean block) {
    final Set<Bytes> unflushedKeys = new HashSet<>();
    final List<UnflushedRecord> remainingUnflushed = new ArrayList<>();
    for (final UnflushedRecord unflushedRecord : unflushed.records()) {
      if (unflushedKeys.contains(Bytes.wrap(unflushedRecord.key()))) {
        // we already have an outstanding process call for this key, wait for it to finish
        remainingUnflushed.add(unflushedRecord);
        continue;
      }
      unflushedKeys.add(Bytes.wrap(unflushedRecord.key()));
      if (!finalizers.containsKey(unflushedRecord.offset())) {
        finalizers.put(
            unflushedRecord.offset(),
            wrapped.processAsync(asyncStores, new Record<>(
                keySerde.deserializer().deserialize("", unflushedRecord.key()),
                valSerde.deserializer().deserialize("", unflushedRecord.value()),
                unflushedRecord.timestamp()
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
        unflushedKeys.remove(Bytes.wrap(unflushedRecord.key()));
      } else {
        remainingUnflushed.add(unflushedRecord);
      }
    }
    unflushedStore.put(
        unflushedKey,
        new UnflushedRecords(ImmutableList.copyOf(remainingUnflushed))
    );
  }

  @Override
  public void process(final Record<KIn, VIn> record) {
    final Optional<RecordMetadata> maybeMetadata = context.recordMetadata();
    if (maybeMetadata.isEmpty()) {
      throw new IllegalStateException("cannot use async processor with upstream punctuation");
    }
    final long offset = maybeMetadata.get().offset();
    UnflushedRecords unflushed = unflushedStore.get(unflushedKey);
    if (unflushed == null) {
      unflushed = new UnflushedRecords(List.of());
    }
    final UnflushedRecords withNext = new UnflushedRecords(
        ImmutableList.<UnflushedRecord>builder()
            .addAll(unflushed.records())
            .add(new UnflushedRecord(
                offset,
                record.timestamp(),
                keySerde.serializer().serialize("", record.key()),
                valSerde.serializer().serialize("", record.value())))
            .build()
    );
    unflushedStore.put(unflushedKey, withNext);
    pokeUnflushed(withNext, withNext.records().size() >= maxUnflushed);
  }

  @Override
  public void close() {
    wrapped.close();
  }

}
