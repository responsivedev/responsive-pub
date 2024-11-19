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

package dev.responsive.examples.regression.tests;

import static dev.responsive.examples.regression.RegConstants.ORDERS;

import dev.responsive.examples.common.InjectedE2ETestException;
import dev.responsive.examples.e2etest.Params;
import dev.responsive.examples.e2etest.UrandomGenerator;
import dev.responsive.examples.regression.RegressionSchema;
import dev.responsive.examples.regression.model.GroupedOrder;
import dev.responsive.examples.regression.model.Order;
import dev.responsive.examples.regression.model.StoredOrder;
import dev.responsive.kafka.api.stores.ResponsiveStores;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;

public class KeyBatchExample extends AbstractKSExampleService {

  private final UrandomGenerator randomGenerator = new UrandomGenerator();

  public KeyBatchExample(final Map<String, Object> props, final boolean responsive) {
    super(
        "key-batch-example",
        props,
        responsive
    );
  }

  @SuppressWarnings("deprecation") // using Transformer interface for compatibility testing
  @Override
  protected Topology buildTopology() {
    final StreamsBuilder builder = new StreamsBuilder();
    if (responsive) {
      builder.addStateStore(ResponsiveStores.keyValueStoreBuilder(
          ResponsiveStores.keyValueStore("grouped-orders-store"),
          Serdes.String(),
          RegressionSchema.storedOrderSerde()
      ));
    } else {
      builder.addStateStore(Stores.keyValueStoreBuilder(
          Stores.inMemoryKeyValueStore("grouped-orders-store"),
          Serdes.String(),
          RegressionSchema.storedOrderSerde()
      ));
    }

    builder.stream(ORDERS, Consumed.with(Serdes.String(), RegressionSchema.orderSerde()))
        .process(BatchTransformer::new, "grouped-orders-store")
        .peek((k, v) -> {
          if (responsive) {
            final var random = Math.abs(randomGenerator.nextLong() % 10000);
            if (random < Params.EXCEPTION_INJECT_THRESHOLD) {
              throw new InjectedE2ETestException();
            }
          }
        })
        .to(resultsTopic(), Produced.with(Serdes.String(), RegressionSchema.groupedOrderSerde()));

    return builder.build();
  }

  private static class BatchTransformer implements Processor<String, Order, String, GroupedOrder> {

    private ProcessorContext<String, GroupedOrder> context;
    private KeyValueStore<String, StoredOrder> store;

    @Override
    public void init(final ProcessorContext<String, GroupedOrder> context) {
      this.context = context;
      this.store = context.getStateStore("grouped-orders-store");
      this.context.schedule(
          Duration.ofSeconds(30),
          PunctuationType.STREAM_TIME,
          this::flushExpired
      );
    }

    @Override
    public void process(final Record<String, Order> record) {
      final long ts = record.timestamp();

      // first add the order to the list of orders that are stored
      store.put(
          storedKey(record.key(), ts),
          new StoredOrder(Optional.of(record.value()), Optional.empty())
      );

      // next, we need to update the tracked metadata row to
      // check whether the value ought to be emitted
      final String mKey = metaKey(record.key());
      final StoredOrder.Meta meta = Optional.ofNullable(store.get(mKey))
          .orElse(new StoredOrder(Optional.empty(), Optional.of(new StoredOrder.Meta(ts, 0, 0))))
          .meta()
          .orElseThrow();

      // instead of computing the actual size, for now just
      // use the value amount and assume that it should be emitted
      // after a certain amount of $$$ is spent
      final StoredOrder.Meta newMeta = new StoredOrder.Meta(
          ts,
          meta.count() + 1,
          meta.size() + (long) record.value().amount()
      );

      if (shouldFlush(newMeta, ts)) {
        doFlush(record.key());
        store.delete(mKey);
      } else {
        store.put(mKey, new StoredOrder(Optional.empty(), Optional.of(newMeta)));
      }
    }

    private void flushExpired(long ts) {
      // iterate through all the metadata keys and check whether
      // the corresponding values should be flushed - we can end
      // at "null" because all keys that are not metadata keys
      // start with "k." (which is less than "m.")
      try (KeyValueIterator<String, StoredOrder> range = store.range("m.", "n.")) {
        while (range.hasNext()) {
          final KeyValue<String, StoredOrder> kv = range.next();
          final StoredOrder.Meta meta = kv.value.meta()
              .orElseThrow(() -> new IllegalStateException(
                  "Got stored meta key with no meta: " + kv));
          if (shouldFlush(meta, ts)) {
            doFlush(kv.key.split("\\.")[1]);
            store.delete(kv.key);
          }
        }
      }
    }

    private void doFlush(final String key) {
      try (
          KeyValueIterator<String, StoredOrder> range = store.range(
              storedKey(key, 0),
              storedKey(key, Long.MAX_VALUE)
          )
      ) {
        final GroupedOrder result = new GroupedOrder(new ArrayList<>());

        while (range.hasNext()) {
          final KeyValue<String, StoredOrder> kv = range.next();
          store.delete(kv.key);

          final StoredOrder value = kv.value;
          result.orders()
              .add(value.order()
                  .orElseThrow(() -> new IllegalStateException(
                      "Got stored order with no order! %s".formatted(value))));
        }

        context.forward(new Record<>(key, result, 0L));
      }
    }

    @Override
    public void close() {

    }

    static boolean shouldFlush(final StoredOrder.Meta meta, final long now) {
      return ((meta.timestamp() - now) > 60_000)
          || (meta.count() > 50)
          || (meta.size() > 1_000);
    }

    static String metaKey(final String key) {
      return "m." + key;
    }

    static String storedKey(final String key, final long ts) {
      return "s.%s.%d".formatted(key, ts);
    }
  }
}
