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

package dev.responsive.kafka.api;

import static dev.responsive.kafka.api.async.AsyncFixedKeyProcessorSupplier.createAsyncProcessorSupplier;
import static dev.responsive.kafka.testutils.processors.Deduplicator.deduplicatorApp;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import dev.responsive.kafka.api.config.ResponsiveConfig;
import dev.responsive.kafka.api.stores.ResponsiveKeyValueParams;
import dev.responsive.kafka.api.stores.ResponsiveStores;
import dev.responsive.kafka.api.stores.TtlProvider;
import dev.responsive.kafka.api.stores.TtlProvider.TtlDuration;
import dev.responsive.kafka.internal.stores.SchemaTypes;
import dev.responsive.kafka.internal.stores.SchemaTypes.KVSchema;
import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

public class ResponsiveTopologyTestDriverKeyValueStoreTest {

  private static final Instant STARTING_TIME = Instant.EPOCH;
  private static final String STORE_NAME = "people";

  private static ResponsiveKeyValueParams paramsForType(final KVSchema type) {
    return type == KVSchema.KEY_VALUE
        ? ResponsiveKeyValueParams.keyValue(STORE_NAME)
        : ResponsiveKeyValueParams.fact(STORE_NAME);
  }

  @ParameterizedTest
  @EnumSource(SchemaTypes.KVSchema.class)
  public void shouldRunWithoutResponsiveConnectionAndNoTtl(final KVSchema type) {
    // Given:
    final Topology topology = topology(paramsForType(type));
    try (final ResponsiveTopologyTestDriver driver = setupDriver(topology)) {

      final TestInputTopic<String, String> bids = driver.createInputTopic(
          "bids", new StringSerializer(), new StringSerializer());
      final TestInputTopic<String, String> people = driver.createInputTopic(
          "people", new StringSerializer(), new StringSerializer());
      final TestOutputTopic<String, String> output = driver.createOutputTopic(
          "output", new StringDeserializer(), new StringDeserializer());

      // When:
      people.pipeInput("1", "1,alice,CA");
      people.pipeInput("2", "2,bob,OR");
      people.pipeInput("3", "3,carol,CA");

      bids.pipeInput("a", "a,100,1");
      bids.pipeInput("b", "b,101,2");
      bids.pipeInput("c", "c,102,1");
      bids.pipeInput("d", "d,103,3");

      // Then:
      final List<String> outputs = output.readValuesToList();
      assertThat(outputs, Matchers.contains(
          "a,100,1,1,alice,CA",
          "c,102,1,1,alice,CA",
          "d,103,3,3,carol,CA"
      ));
    }
  }

  @ParameterizedTest
  @EnumSource(SchemaTypes.KVSchema.class)
  public void shouldEnforceKeyBasedTtlByAdvancingStreamTime(final KVSchema type) {
    // Given:
    final Duration defaultTtl = Duration.ofMillis(15);

    // Apply infinite retention only to key (ie "person id") of 0, everyone else is default
    final TtlProvider<String, ?> ttlProvider = TtlProvider.<String, Object>withDefault(defaultTtl)
        .fromKey(k -> {
          if (k.equals("0")) {
            return Optional.of(TtlDuration.infinite());
          } else {
            return Optional.empty();
          }
        });

    final Topology topology = topology(paramsForType(type).withTtlProvider(ttlProvider));
    try (final TopologyTestDriver driver = setupDriver(topology)) {

      final TestInputTopic<String, String> bids = driver.createInputTopic(
          "bids", new StringSerializer(), new StringSerializer());
      final TestInputTopic<String, String> people = driver.createInputTopic(
          "people", new StringSerializer(), new StringSerializer());
      final TestOutputTopic<String, String> output = driver.createOutputTopic(
          "output", new StringDeserializer(), new StringDeserializer());

      // When:
      people.pipeInput("0", "0,infinite,CA", 0);  // insert time = 0
      people.pipeInput("1", "1,alice,CA", 0);     // insert time = 0
      people.pipeInput("2", "2,bob,OR", 5);       // insert time = 5 (advances streamTime to 5)
      people.pipeInput("3", "3,carol,CA", 10);    // insert time = 10 (advances streamTime to 10)

      bids.pipeInput("a", "a,100,1", 10);      // streamTime = 10 -- result as alice is not expired
      bids.pipeInput("b", "b,101,2", 10);      // streamTime = 10 -- result as bob is not expired

      // advance streamTime to 20
      bids.pipeInput("c", "c,102,1", 20);      // streamTime = 20 -- no result b/c alice has expired
      bids.pipeInput("d", "d,103,3", 20);      // streamTime = 20 -- result as carol is not expired

      people.pipeInput("1", "1,alex,CA", 20);  // insert streamTime = 20
      bids.pipeInput(
          "e", "e,104,1", 20);      // streamTime = 20 -- yes result as alex replaced alice

      // advance streamTime to 30
      bids.pipeInput("f", "f,105,3", 30);      // streamTime = 30 -- no result b/c carol has expired

      bids.pipeInput("g", "g,106,0", 30);      // streamTime = 30 -- yes result b/c id 0 is infinite

      // Then:
      final List<String> outputs = output.readValuesToList();
      assertThat(outputs, Matchers.contains(
          "a,100,1,1,alice,CA",
          "d,103,3,3,carol,CA",
          "e,104,1,1,alex,CA",
          "g,106,0,0,infinite,CA"
      ));
    }

  }

  @ParameterizedTest
  @EnumSource(SchemaTypes.KVSchema.class)
  public void shouldEnforceKeyBasedTtlByAdvancingWallclockTime(final KVSchema type) {
    // Given:
    final Duration defaultTtl = Duration.ofMillis(15);

    // Apply infinite retention only to key (ie "person id") of 0, everyone else is default
    final TtlProvider<String, ?> ttlProvider = TtlProvider.<String, Object>withDefault(defaultTtl)
        .fromKey(k -> {
          if (k.equals("0")) {
            return Optional.of(TtlDuration.infinite());
          } else {
            return Optional.empty();
          }
        });
    final Topology topology = topology(paramsForType(type).withTtlProvider(ttlProvider));
    try (final TopologyTestDriver driver = setupDriver(topology)) {

      final TestInputTopic<String, String> bids = driver.createInputTopic(
          "bids", new StringSerializer(), new StringSerializer());
      final TestInputTopic<String, String> people = driver.createInputTopic(
          "people", new StringSerializer(), new StringSerializer());
      final TestOutputTopic<String, String> output = driver.createOutputTopic(
          "output", new StringDeserializer(), new StringDeserializer());

      // When:
      people.pipeInput("0", "0,infinite,CA", 0);  // insert time = 0
      people.pipeInput("1", "1,alice,CA", 0);     // insert time = 0
      people.pipeInput("2", "2,bob,OR", 5);       // insert time = 5
      people.pipeInput("3", "3,carol,CA", 10);    // insert time = 10

      bids.pipeInput("a", "a,100,1", 10);      // streamTime = 10 -- result as alice is not expired
      bids.pipeInput("b", "b,101,2", 10);      // streamTime = 10 -- result as bob is not expired

      driver.advanceWallClockTime(Duration.ofMillis(20)); // advances wallclock time to 20

      bids.pipeInput("c", "c,102,1", 20);       // time = 20 -- no result b/c alice has expired
      bids.pipeInput("d", "d,103,3", 20);       // time = 20 -- result since carol is not expired

      people.pipeInput("1", "1,alex,CA", 20);   // insert time = 20
      bids.pipeInput("e", "e,104,1", 20);       // time = 20 -- result as alex has replaced alice

      driver.advanceWallClockTime(Duration.ofMillis(30)); // advances wallclock time to 30

      bids.pipeInput("f", "f,105,3", 30);      // time = 30 -- no result b/c carol has expired

      bids.pipeInput("g", "g,106,0", 30);      // time = 30 -- result b/c person w/ id 0 is infinite

      // Then:
      final List<String> outputs = output.readValuesToList();
      assertThat(outputs, Matchers.containsInAnyOrder(
          "a,100,1,1,alice,CA",
          "d,103,3,3,carol,CA",
          "e,104,1,1,alex,CA",
          "g,106,0,0,infinite,CA"
      ));
    }
  }

  @ParameterizedTest
  @EnumSource(SchemaTypes.KVSchema.class)
  public void shouldDeduplicateWithTtlProviderToExpireOldRecords(final KVSchema type) {
    final String value = "ignored";

    final String key = "key";
    final long ttlMs = 10_000L; // 10s
    final var ttlProvider =
        TtlProvider.withDefault(Duration.ofMillis(ttlMs))
            .fromKey(
                k -> {
                  final Duration ttl = Duration.ofMillis(ttlMs);
                  System.out.println("TTL: " + ttl);
                  return Optional.of(TtlDuration.of(ttl));
                });

    final ResponsiveKeyValueParams params = paramsForType(type).withTtlProvider(ttlProvider);
    final Topology topology = deduplicatorApp("input", "output", params);
    try (final ResponsiveTopologyTestDriver driver = setupDriver(topology)) {

      final TestInputTopic<String, String> inputTopic = driver.createInputTopic(
          "input",
          new StringSerializer(),
          new StringSerializer(),
          STARTING_TIME,
          Duration.ZERO
      );
      inputTopic.pipeInput(key, value);

      final KeyValueStore<String, ValueAndTimestamp<String>> transactionIdStore =
          driver.getTimestampedKeyValueStore(STORE_NAME);

      assertNotNull(
          transactionIdStore.get(key), "should have a single txn id in state store");

      // advance wall clock time by 1 millisecond past expiration
      driver.advanceWallClockTime(Duration.ofMillis(ttlMs).plusMillis(1L));

      // should be expired
      assertNull(transactionIdStore.get(key), "should have no txn id in state store");

      // send the same event in again, outside the dedupe window
      inputTopic.pipeInput(key, value, ttlMs + 1);
      assertNotNull(
          transactionIdStore.get(key), "should have a single txn id in state store, again");
    }
  }

  @Test
  public void shouldLoadPropertiesFromFile() throws Exception {
    final String propsPath = "ttd-app.properties";
    final InputStream inputStream = getClass().getClassLoader().getResourceAsStream(propsPath);

    if (inputStream == null) {
      throw new RuntimeException("ttd-app.properties not found under test/resources/");
    }

    final Properties properties = new Properties();
    properties.load(inputStream);

    final var params = ResponsiveKeyValueParams.keyValue("store");
    final Topology topology = deduplicatorApp("input", "output", params);
    try (final var driver = new ResponsiveTopologyTestDriver(topology, properties, STARTING_TIME)) {
      final TestInputTopic<String, String> inputTopic = driver.createInputTopic(
          "input",
          new StringSerializer(),
          new StringSerializer(),
          STARTING_TIME,
          Duration.ZERO
      );

      final TestOutputTopic<String, String> output = driver.createOutputTopic(
          "output", new StringDeserializer(), new StringDeserializer());

      inputTopic.pipeInput("A", "A1");
      inputTopic.pipeInput("A", "A2");
      inputTopic.pipeInput("B", "B1");
      inputTopic.pipeInput("A", "A3");

      final List<String> outputs = output.readValuesToList();
      assertThat(outputs.size(), equalTo(2));
      assertThat(outputs, Matchers.containsInAnyOrder(
          "A1",
          "B1"
      ));
    }

  }

  private ResponsiveTopologyTestDriver setupDriver(final Topology topology) {
    final Properties props = new Properties();
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
    props.put(ResponsiveConfig.ASYNC_THREAD_POOL_SIZE_CONFIG, 2);

    return new ResponsiveTopologyTestDriver(topology, props, STARTING_TIME);
  }

  private Topology topology(final ResponsiveKeyValueParams storeParams) {
    final StreamsBuilder builder = new StreamsBuilder();

    // schema for bids is key: <bid_id> value: <bid_id, amount, person_id>
    final KStream<String, String> bids = builder.stream("bids");
    // schema for people is key: <person_id> value: <person_id, name, state>
    final KTable<String, String> people = builder.table(
        STORE_NAME,
        ResponsiveStores.materialized(storeParams)
    );

    bids
        // person_id is 3rd column
        .selectKey((k, v) -> v.split(",")[2])
        // schema is now <bid_id, amount, person_id, name, state>
        .join(people, (bid, person) -> bid + "," + person)
        // state is the 6th column
        .filter((k, v) -> v.split(",")[5].equals("CA"))
        .processValues(createAsyncProcessorSupplier(
            () -> new FixedKeyProcessor<String, String, String>() {

              private FixedKeyProcessorContext<String, String> context;

              @Override
              public void init(final FixedKeyProcessorContext<String, String> context) {
                this.context = context;
              }

              @Override
              public void process(final FixedKeyRecord<String, String> fixedKeyRecord) {
                context.forward(fixedKeyRecord);
              }
            }))
        .to("output");

    return builder.build();
  }

}
