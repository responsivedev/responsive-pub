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

package dev.responsive.kafka.integration;

import static dev.responsive.kafka.testutils.IntegrationTestUtils.pipeTimestampedRecords;
import static dev.responsive.kafka.testutils.IntegrationTestUtils.startAppAndAwaitRunning;
import static org.apache.kafka.streams.StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG;
import static org.hamcrest.MatcherAssert.assertThat;

import dev.responsive.kafka.api.ResponsiveKafkaStreams;
import dev.responsive.kafka.api.config.StorageBackend;
import dev.responsive.kafka.api.stores.ResponsiveKeyValueParams;
import dev.responsive.kafka.api.stores.ResponsiveStores;
import dev.responsive.kafka.testutils.IntegrationTestUtils;
import dev.responsive.kafka.testutils.KeyValueTimestamp;
import dev.responsive.kafka.testutils.ResponsiveConfigParam;
import dev.responsive.kafka.testutils.ResponsiveExtension;
import dev.responsive.kafka.testutils.StoreComparatorSuppliers;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.internals.RocksDBKeyValueBytesStoreSupplier;
import org.hamcrest.Matchers;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.RegisterExtension;

public class ResponsiveKeyValueStoreIntegrationTest {

  @RegisterExtension
  static ResponsiveExtension EXTENSION = new ResponsiveExtension(StorageBackend.MONGO_DB);

  private static final String INPUT_TOPIC = "input";
  private static final String OUTPUT_TOPIC = "output";

  private final Map<String, Object> responsiveProps = new HashMap<>();

  private String name;

  @BeforeEach
  public void before(
      final TestInfo info,
      final Admin admin,
      @ResponsiveConfigParam final Map<String, Object> responsiveProps
  ) throws InterruptedException, ExecutionException {
    // add displayName to name to account for parameterized tests
    name = info.getTestMethod().orElseThrow().getName() + "-" + new Random().nextInt();

    this.responsiveProps.putAll(responsiveProps);

    final var result = admin.createTopics(
        List.of(
            new NewTopic(inputTopic(), Optional.of(2), Optional.empty()),
            new NewTopic(outputTopic(), Optional.of(1), Optional.empty())
        )
    );
    result.all().get();
  }

  private String inputTopic() {
    return name + "." + INPUT_TOPIC;
  }

  private String outputTopic() {
    return name + "." + OUTPUT_TOPIC;
  }

  /*
   * This test makes sure that the default RocksDB state store and the responsive state
   * store consistently show identical internal behavior.
   * We do not check the output topic but rather use the StoreComparator to ensure that
   * they return identical results from each method invoked on them.
   */
  @Test
  public void shouldMatchRocksDB() throws Exception {
    final StoreComparatorSuppliers.MultiKeyValueStoreSupplier
        multiKeyValueStoreSupplier = multiKeyValueStoreSupplier(name);

    final Materialized<String, String, KeyValueStore<Bytes, byte[]>> combinedStore =
        Materialized.as(multiKeyValueStoreSupplier);

    final StoreBuilder<KeyValueStore<String, String>> storeBuilder =
        Stores.keyValueStoreBuilder(
            multiKeyValueStoreSupplier(name + "2"),
            Serdes.String(),
            Serdes.String());

    // Start from timestamp of 0L to get predictable results
    final List<KeyValueTimestamp<String, String>> inputEvents = Arrays.asList(
        new KeyValueTimestamp<>("key", "a", 0L),
        new KeyValueTimestamp<>("keyB", "x", 0L),
        new KeyValueTimestamp<>("keyC", "y", 0L),
        new KeyValueTimestamp<>("keyD", "z", 0L),

        new KeyValueTimestamp<>("key", "c", 1_000L),
        new KeyValueTimestamp<>("keyB", "x", 1_200L),
        new KeyValueTimestamp<>("keyC", "y", 1_300L),
        new KeyValueTimestamp<>("keyD", "z", 1_400L),

        new KeyValueTimestamp<>("key", "b", 2_000L),
        new KeyValueTimestamp<>("keyB", "x", 2_200L),
        new KeyValueTimestamp<>("keyC", "y", 2_300L),
        new KeyValueTimestamp<>("keyD", "z", 2_400L),

        new KeyValueTimestamp<>("key", "d", 3_000L),
        new KeyValueTimestamp<>("key", "b", 3_000L),
        new KeyValueTimestamp<>("key", null, 4_000L),

        new KeyValueTimestamp<>("key2", "e", 4_000L),
        new KeyValueTimestamp<>("key2B", "x", 4_200L),
        new KeyValueTimestamp<>("key2C", "y", 4_300L),
        new KeyValueTimestamp<>("key2D", "z", 4_400L),

        new KeyValueTimestamp<>("key2", "b", 5_000L),

        new KeyValueTimestamp<>("STOP", "b", 18_000L)
    );
    final CountDownLatch outputLatch = new CountDownLatch(1);

    final StreamsBuilder builder = new StreamsBuilder();
    builder.addStateStore(storeBuilder);

    final KStream<String, String> input = builder.stream(inputTopic());
    input
        // add a processor that issues a range scan on a KV state store
        // since there are no DSL methods for this we have to do it
        // via the process API
        .process(new ProcessorSupplier<String, String, String, String>() {
          @Override
          public Set<StoreBuilder<?>> stores() {
            return Set.of(storeBuilder);
          }

          @Override
          public Processor<String, String, String, String> get() {
            return new Processor<>() {

              private ProcessorContext<String, String> context;
              private KeyValueStore<String, String> store;

              @Override
              public void init(final ProcessorContext<String, String> context) {
                this.store = context.getStateStore(storeBuilder.name());
                this.context = context;
              }

              @Override
              public void process(final Record<String, String> record) {
                store.put(record.key(), record.value());
                if (record.value() == null) {
                  context.forward(record);
                  return;
                }

                final StringBuilder combined;
                try (
                    KeyValueIterator<String, String> range = store.range(
                        record.key(),
                        record.key() + "Z"
                    )
                ) {
                  combined = new StringBuilder(record.value());
                  while (range.hasNext()) {
                    final KeyValue<String, String> next = range.next();
                    combined.append(next.value);
                  }
                }

                context.forward(record.withValue(combined.toString()));
              }
            };
          }
        })
        .groupByKey()
        .aggregate(() -> "", (k, v1, agg) -> agg + v1, combinedStore)
        .toStream()
        .peek((k, v) -> {
          if (k.equals("STOP")) {
            outputLatch.countDown();
          }
        })
        .selectKey((k, v) -> k)
        .to(outputTopic());

    // When:
    final Map<String, Object> properties =
        IntegrationTestUtils.getDefaultMutablePropertiesWithStringSerdes(responsiveProps, name);
    properties.put(STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
    final KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
    try (
        final ResponsiveKafkaStreams kafkaStreams =
            new ResponsiveKafkaStreams(builder.build(), properties)
    ) {
      startAppAndAwaitRunning(Duration.ofSeconds(15), kafkaStreams);
      pipeTimestampedRecords(producer, inputTopic(), inputEvents);

      final long maxWait = inputEvents.get(inputEvents.size() - 1).timestamp() + 2_000;
      assertThat(
          outputLatch.await(maxWait, TimeUnit.MILLISECONDS),
          Matchers.equalTo(true)
      );
    }
  }

  @NotNull
  private StoreComparatorSuppliers.MultiKeyValueStoreSupplier multiKeyValueStoreSupplier(
      final String name
  ) {
    final KeyValueBytesStoreSupplier rocksDbStore =
        new RocksDBKeyValueBytesStoreSupplier(name, false);

    final KeyValueBytesStoreSupplier responsiveStore =
        ResponsiveStores.keyValueStore(ResponsiveKeyValueParams.keyValue(name));

    final StoreComparatorSuppliers.CompareFunction compare =
        (String method, Object[] args, Object actual, Object truth) -> {
          final String reason = method + " should yield identical results.";
          assertThat(reason, actual, Matchers.equalTo(truth));
        };

    return new StoreComparatorSuppliers.MultiKeyValueStoreSupplier(
        rocksDbStore,
        responsiveStore,
        compare
    );
  }

}
