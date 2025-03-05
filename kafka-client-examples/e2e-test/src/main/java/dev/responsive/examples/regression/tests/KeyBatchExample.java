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
import dev.responsive.examples.regression.model.OrderMetadata;
import dev.responsive.kafka.api.stores.ResponsiveStores;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

public class KeyBatchExample extends AbstractKSExampleService {

  private static final String METADATA_STORE_NAME = "metadata";
  private static final String PURCHASES_STORE_NAME = "purchases";

  private final UrandomGenerator randomGenerator = new UrandomGenerator();

  public KeyBatchExample(final Map<String, Object> props, final boolean responsive) {
    super(
        "key-batch-example",
        props,
        responsive
    );
  }

  @Override
  protected Topology buildTopology() {
    final StreamsBuilder builder = new StreamsBuilder();

    builder.stream(ORDERS, Consumed.with(Serdes.String(), RegressionSchema.orderSerde()))
        .process(new BatchProcessorSupplier(responsive), PURCHASES_STORE_NAME, METADATA_STORE_NAME)
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

  private static class BatchProcessorSupplier implements
      ProcessorSupplier<String, Order, String, GroupedOrder> {

    final boolean responsive;

    public BatchProcessorSupplier(final boolean responsive) {
      this.responsive = responsive;
    }

    @Override
    public Processor<String, Order, String, GroupedOrder> get() {
      return new BatchProcessor();
    }

    @Override
    public Set<StoreBuilder<?>> stores() {
      if (responsive) {
        return Set.of(
            ResponsiveStores.keyValueStoreBuilder(
                ResponsiveStores.keyValueStore(PURCHASES_STORE_NAME),
                Serdes.String(),
                RegressionSchema.orderSerde()
            ),
            ResponsiveStores.keyValueStoreBuilder(
                ResponsiveStores.keyValueStore(METADATA_STORE_NAME),
                Serdes.String(),
                RegressionSchema.orderMetadataSerde()
            )
        );
      } else {
        return Set.of(
            Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore(PURCHASES_STORE_NAME),
                Serdes.String(),
                RegressionSchema.orderSerde()
            ),
            Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore(METADATA_STORE_NAME),
                Serdes.String(),
                RegressionSchema.orderMetadataSerde()
            )
        );
      }
    }
  }

  // TODO: use FixedKeyProcessor after fixing https://issues.apache.org/jira/browse/KAFKA-16585
  private static class BatchProcessor implements Processor<String, Order, String, GroupedOrder> {

    private ProcessorContext<String, GroupedOrder> context;
    private KeyValueStore<String, Order> purchasesStore;
    private KeyValueStore<String, OrderMetadata> metadataStore;

    @Override
    public void init(final ProcessorContext<String, GroupedOrder> context) {
      this.context = context;
      this.purchasesStore = context.getStateStore(PURCHASES_STORE_NAME);
      this.metadataStore = context.getStateStore(METADATA_STORE_NAME);
      this.context.schedule(
          Duration.ofSeconds(30),
          PunctuationType.STREAM_TIME,
          this::flushReadyOrders
      );
    }

    @Override
    public void process(final Record<String, Order> newPurchase) {
      final String key = newPurchase.key();
      final long newPurchaseTimestamp = newPurchase.timestamp();
      final long newPurchaseSize = (long) newPurchase.value().amount();

      // first store the purchase under the key+timestamp
      purchasesStore.put(storedKey(key, newPurchaseTimestamp), newPurchase.value());

      // next, we need to look up and update the tracked metadata for this key
      final OrderMetadata orderMetadata = metadataStore.get(key);

      final OrderMetadata newOrderMetadata =
          orderMetadata == null
              ? new OrderMetadata(newPurchaseTimestamp, 1, newPurchaseSize)
              : new OrderMetadata(
                  orderMetadata.timestamp(),
                  orderMetadata.count() + 1,
                  orderMetadata.size() + newPurchaseSize
              );

      // check if the key's purchases are ready to be batched and flushed,
      // otherwise just overwrite the metadata row with the new info
      if (shouldFlush(newOrderMetadata, context.currentStreamTimeMs())) {
        doFlush(key, newOrderMetadata.timestamp());
      } else {
        metadataStore.put(key, newOrderMetadata);
      }
    }

    private void flushReadyOrders(final long now) {
      // iterate through all the metadata rows and check whether the purchases
      // for each key are ready to be batched and flushed
      try (KeyValueIterator<String, OrderMetadata> range = metadataStore.all()) {
        while (range.hasNext()) {
          final KeyValue<String, OrderMetadata> kv = range.next();
          final OrderMetadata orderMetadata = kv.value;
          if (shouldFlush(orderMetadata, now)) {
            doFlush(kv.key, orderMetadata.timestamp());
          }
        }
      }
    }

    private void doFlush(final String key, final long batchTimestamp) {
      try (
          KeyValueIterator<String, Order> range = purchasesStore.range(
              storedKey(key, 0),
              storedKey(key, Long.MAX_VALUE)
          )
      ) {
        final GroupedOrder groupedOrder = new GroupedOrder(new ArrayList<>());

        while (range.hasNext()) {
          final KeyValue<String, Order> kv = range.next();
          purchasesStore.delete(kv.key);
          groupedOrder.orders().add(kv.value);
        }

        context.forward(new Record<>(key, groupedOrder, batchTimestamp));
      }

      // make sure to delete from the metadata store once the key is fully flushed
      metadataStore.delete(key);
    }

    private static boolean shouldFlush(final OrderMetadata orderMetadata, final long now) {
      return ((orderMetadata.timestamp() - now) > 60_000)
          || (orderMetadata.count() > 50)
          || (orderMetadata.size() > 1_000);
    }

    private static String storedKey(final String key, final long ts) {
      return "%s.%d".formatted(key, ts);
    }
  }
}
