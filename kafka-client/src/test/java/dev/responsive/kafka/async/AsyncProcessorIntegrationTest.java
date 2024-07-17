/*
 * Copyright 2024 Responsive Computing, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package dev.responsive.kafka.async;

import static dev.responsive.kafka.api.async.AsyncFixedKeyProcessorSupplier.createAsyncProcessorSupplier;
import static dev.responsive.kafka.api.config.ResponsiveConfig.ASYNC_FLUSH_INTERVAL_MS_CONFIG;
import static dev.responsive.kafka.api.config.ResponsiveConfig.ASYNC_MAX_EVENTS_QUEUED_PER_ASYNC_THREAD_CONFIG;
import static dev.responsive.kafka.api.config.ResponsiveConfig.ASYNC_MAX_EVENTS_QUEUED_PER_KEY_CONFIG;
import static dev.responsive.kafka.api.config.ResponsiveConfig.ASYNC_THREAD_POOL_SIZE_CONFIG;
import static dev.responsive.kafka.api.config.ResponsiveConfig.STORE_FLUSH_INTERVAL_TRIGGER_MS_CONFIG;
import static dev.responsive.kafka.api.config.ResponsiveConfig.STORE_FLUSH_RECORDS_TRIGGER_CONFIG;
import static dev.responsive.kafka.testutils.IntegrationTestUtils.createTopicsAndWait;
import static dev.responsive.kafka.testutils.IntegrationTestUtils.pipeRecords;
import static dev.responsive.kafka.testutils.IntegrationTestUtils.readOutput;
import static dev.responsive.kafka.testutils.IntegrationTestUtils.startAppAndAwaitRunning;
import static dev.responsive.kafka.testutils.IntegrationTestUtils.startAppAndAwaitState;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.TRANSACTION_TIMEOUT_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.COMMIT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.EXACTLY_ONCE_V2;
import static org.apache.kafka.streams.StreamsConfig.NUM_STREAM_THREADS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.PROCESSING_GUARANTEE_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.consumerPrefix;
import static org.apache.kafka.streams.StreamsConfig.producerPrefix;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import dev.responsive.kafka.api.ResponsiveKafkaStreams;
import dev.responsive.kafka.api.config.StorageBackend;
import dev.responsive.kafka.api.stores.ResponsiveKeyValueParams;
import dev.responsive.kafka.api.stores.ResponsiveStores;
import dev.responsive.kafka.testutils.ResponsiveConfigParam;
import dev.responsive.kafka.testutils.ResponsiveExtension;
import dev.responsive.kafka.testutils.SimpleStatefulProcessorSupplier;
import dev.responsive.kafka.testutils.SimpleStatefulProcessorSupplier.SimpleProcessorOutput;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.RegisterExtension;

public class AsyncProcessorIntegrationTest {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @RegisterExtension
  static ResponsiveExtension EXTENSION = new ResponsiveExtension(StorageBackend.CASSANDRA);

  ////////////////// Integration Test Toggles //////////////////

  // Test parameters:
  // How long to sleep to mock a long/blocking call
  private static final long ASYNC_SLEEP_DURATION_MS = 10L;
  // Ratio of input topic partition count to output topic
  private static final int INPUT_OUTPUT_PARTITION_RATIO = 3;
  // How many stores in the async processor
  private static final int NUM_ASYNC_STORES = 1; // TODO: not implemented, changing it has no effect

  /// StreamsConfigs:
  // commit.interval.ms (default 30s)
  private static final long COMMIT_INTERVAL_MS = 30 * 1000L;

  // ResponsiveConfigs:
  // responsive.store.flush.trigger.local.interval (default 30s)
  private static final long COMMIT_BUFFER_FLUSH_INTERVAL_MS = Long.MAX_VALUE;
  // responsive.store.flush.trigger.local.records (default MAX_VALUE)
  private static final int COMMIT_BUFFER_FLUSH_RECORDS = 0;
  // responsive.async.max.events.queued.per.async.thread
  private static final int MAX_EVENTS_QUEUED_PER_ASYNC_THREAD = 5;
  // responsive.async.max.events.queued.per.key
  private static final int MAX_EVENTS_QUEUED_PER_KEY = 3;
  // responsive.async.flush.interval.ms
  private static final long ASYNC_FLUSH_INTERVAL_MS = 5 * 1000L;

  ///////////// CONSTANTS /////////////

  private static final int STREAMTHREADS_PER_APP = 5;
  private static final int ASYNC_THREADS_PER_STREAMTHREAD = 5;
  private static final int TASKS_PER_STREAMTHREAD = 2; // approximate/assumes balanced partitioning
  private static final int KEYS_PER_TASK = 5; // TODO: implement key list based on this config
  private static final int INPUT_RECORDS_PER_KEY = 10;

  private static final String INPUT_TOPIC = "input";
  private static final String OUTPUT_TOPIC = "output";

  private static final String IN_KV_STORE = "in-kv-store";
  private static final String ASYNC_KV_STORE = "async-kv-store";
  private static final String OUT_KV_STORE = "out-kv-store";

  private final Map<String, Object> responsiveProps = new HashMap<>();

  private String name;
  private Admin admin;
  private int numInputPartitions;
  private int numOutputPartitions;

  @BeforeEach
  public void before(
      final TestInfo info,
      final Admin admin,
      @ResponsiveConfigParam final Map<String, Object> responsiveProps
  ) {
    // add displayName to name to account for parameterized tests
    this.name = info.getDisplayName().replace("()", "");

    this.responsiveProps.putAll(responsiveProps);
    this.admin = admin;

    this.numInputPartitions = TASKS_PER_STREAMTHREAD * STREAMTHREADS_PER_APP;
    this.numOutputPartitions = numInputPartitions / INPUT_OUTPUT_PARTITION_RATIO;
    createTopicsAndWait(
        admin,
        Map.of(inputTopic(), numInputPartitions, outputTopic(), numOutputPartitions)
    );
  }

  @AfterEach
  public void after() {
    admin.deleteTopics(List.of(inputTopic(), outputTopic()));
  }

  private String inputTopic() {
    return name + "." + INPUT_TOPIC;
  }

  private String outputTopic() {
    return name + "." + OUTPUT_TOPIC;
  }

  @Test
  public void shouldProcessStatelessEventsInOrderByKey() throws Exception {
    // Given:
    final List<String> keys = List.of("a", "b", "c", "d", "e", "f", "g");

    // produce N records for each key, with same-key events interleaved between other keys
    final List<KeyValue<String, InputRecord>> inputRecords = new LinkedList<>();
    for (int val = 1; val < 1 + INPUT_RECORDS_PER_KEY; ++val) {
      for (final String key : keys) {
        final InputRecord inputRecord;
        if (val == 2 && key.equals("b")) {
          inputRecord = new InputRecord(key + val, new InjectedFault(
              InjectedFault.Type.EXCEPTION,
              InjectedFault.Frequency.ONCE)
          );
        } else {
          inputRecord = new InputRecord(key + val);
        }
        inputRecords.add(new KeyValue<>(key, inputRecord));
      }
    }

    final Map<String, String> finalOutputRecords = new HashMap<>(keys.size());
    for (final String key : keys) {
      final String finalOutputValue = String.format(
          "%s:IN:%d--PROCESSED--%s:END:%d",
          key, INPUT_RECORDS_PER_KEY, key, INPUT_RECORDS_PER_KEY);

      finalOutputRecords.put(key, finalOutputValue);
    }

    // The total number of records processed, equal to the total number of output records
    // ONLY when caching is disabled
    final int numInputRecords = keys.size() * INPUT_RECORDS_PER_KEY;

    final Map<String, String> latestValues = new ConcurrentHashMap<>();
    final CountDownLatch inputRecordsLatch = new CountDownLatch(numInputRecords);

    final Map<String, Object> properties = getMutableProperties();
    final KafkaProducer<String, InputRecord> producer = new KafkaProducer<>(properties);

    final StreamsBuilder builder = new StreamsBuilder();
    final KStream<String, InputRecord> input = builder.stream(
        inputTopic(),
        Consumed.with(
            Serdes.String(),
            Serdes.serdeFrom(new InputRecordSerializer(), new InputRecordDeserializer())
        ));

    input
        .processValues(
            new SimpleStatefulProcessorSupplier<>(
                this::computeNewValueForSourceProcessor,
                ResponsiveKeyValueParams.fact(IN_KV_STORE)),
            IN_KV_STORE)
        .processValues(
            createAsyncProcessorSupplier(
                () -> new FixedKeyProcessor<String, InputRecord, String>() {
                  private FixedKeyProcessorContext<String, String> context;

                  @Override
                  public void init(final FixedKeyProcessorContext<String, String> context) {
                    this.context = context;
                  }

                  @Override
                  public void process(final FixedKeyRecord<String, InputRecord> record) {
                    final String value = computeNewValueForStatelessAsyncProcessor(record, context);
                    context.forward(record.withValue(value));
                  }
                }),
            Named.as("AsyncProcessor"))
        .processValues(
            new SimpleStatefulProcessorSupplier<>(
                this::computeNewValueForSinkProcessor,
                ResponsiveKeyValueParams.fact(OUT_KV_STORE),
                latestValues,
                inputRecordsLatch),
            OUT_KV_STORE)
        .to(outputTopic(), Produced.with(Serdes.String(), Serdes.String()));

    final List<Throwable> caughtExceptions = new LinkedList<>();
    try (final var streams = new ResponsiveKafkaStreams(builder.build(), properties)) {
      streams.setUncaughtExceptionHandler(exception -> {
        caughtExceptions.add(exception);
        return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
      });
      startAppAndAwaitRunning(Duration.ofSeconds(30), streams);

      // When:
      pipeRecords(producer, inputTopic(), inputRecords);

      // Then:
      final long timeoutMs = 60_000L + ASYNC_SLEEP_DURATION_MS * numInputRecords;
      final boolean allInputProcessed = inputRecordsLatch.await(timeoutMs, TimeUnit.MILLISECONDS);
      if (!allInputProcessed) {
        throw new AssertionError(String.format("Failed to process all %d input records within %dms",
                                               numInputPartitions, timeoutMs));
      }

      final var kvs = readOutput(
          outputTopic(), 0, numInputRecords, numOutputPartitions, false, properties
      );

      final Map<String, String> latestByKey = new HashMap<>();
      for (final var kv : kvs) {
        latestByKey.put((String) kv.key, (String) kv.value);
      }
      for (final String key : keys) {
        final String finalValue = finalOutputRecords.get(key);
        assertThat(latestByKey.get(key), is(finalValue));
      }
    }
    assertThat(caughtExceptions.size(), is(1));
    assertThat(Throwables.getRootCause(caughtExceptions.get(0)),
               instanceOf(InjectedException.class));
    assertThat(latestValues, equalTo(finalOutputRecords));
  }

  @Test
  public void shouldProcessStatefulEventsInOrderByKey() throws Exception {
    // Given:
    final List<String> keys = List.of("a", "b", "c", "d", "e", "f", "g", "h", "i", "j",
                                      "k", "l", "m", "n", "o", "p", "q", "r", "s", "t",
                                      "u", "v", "w", "x", "y", "z",
                                      "aa", "bb", "cc", "dd", "ee", "ff", "gg", "hh", "ii", "jj",
                                      "kk", "ll", "mm", "nn", "oo", "pp", "qq", "rr", "ss", "tt",
                                      "uu", "vv", "ww", "xx", "yy", "zz");

    // produce N records for each key, with same-key events interleaved between other keys
    final List<KeyValue<String, InputRecord>> inputRecords = new LinkedList<>();
    for (int val = 1; val < 1 + INPUT_RECORDS_PER_KEY; ++val) {
      for (final String key : keys) {
        final InputRecord inputRecord;
        if (val == 2 && key.equals("b")) {
          inputRecord = new InputRecord(key + val, new InjectedFault(
              InjectedFault.Type.EXCEPTION,
              InjectedFault.Frequency.ONCE)
          );
        } else {
          inputRecord = new InputRecord(key + val);
        }
        inputRecords.add(new KeyValue<>(key, inputRecord));
      }
    }

    final Map<String, String> finalOutputRecords = new HashMap<>(keys.size());
    for (final String key : keys) {
      final StringBuilder finalValueStringBuilder = new StringBuilder();

      for (int val = 1; val < 1 + INPUT_RECORDS_PER_KEY; ++val) {
        finalValueStringBuilder.append(String.format("%s:IN:%d--", key, val));
      }

      finalValueStringBuilder.append(String.format("%s:END:%d", key, INPUT_RECORDS_PER_KEY));

      finalOutputRecords.put(key, finalValueStringBuilder.toString());
    }

    // The total number of records processed, equal to the total number of output records
    // ONLY when caching is disabled
    final int numInputRecords = keys.size() * INPUT_RECORDS_PER_KEY;

    final AtomicInteger processed = new AtomicInteger(0);
    final Map<String, String> latestValues = new ConcurrentHashMap<>();
    final CountDownLatch inputRecordsLatch = new CountDownLatch(numInputRecords);

    final Map<String, Object> properties = getMutableProperties();
    final KafkaProducer<String, InputRecord> producer = new KafkaProducer<>(properties);

    final StreamsBuilder builder = new StreamsBuilder();
    final KStream<String, InputRecord> input = builder.stream(
        inputTopic(),
        Consumed.with(
            Serdes.String(),
            Serdes.serdeFrom(new InputRecordSerializer(), new InputRecordDeserializer())
        ));

    input
        .processValues(
            new SimpleStatefulProcessorSupplier<>(
                this::computeNewValueForSourceProcessor,
                ResponsiveKeyValueParams.fact(IN_KV_STORE)),
            IN_KV_STORE)
        .processValues(
            createAsyncProcessorSupplier(
                new SimpleStatefulProcessorSupplier<>(
                    this::computeNewValueForStatefulAsyncProcessor,
                    ResponsiveKeyValueParams.fact(ASYNC_KV_STORE),
                    processed
                )),
            Named.as("AsyncProcessor"),
            ASYNC_KV_STORE)
        .processValues(
            new SimpleStatefulProcessorSupplier<>(
                this::computeNewValueForSinkProcessor,
                ResponsiveKeyValueParams.fact(OUT_KV_STORE),
                latestValues,
                inputRecordsLatch),
            OUT_KV_STORE)
        .to(outputTopic(), Produced.with(Serdes.String(), Serdes.String()));

    List<Throwable> caughtExceptions = new LinkedList<>();
    try (final var streams = new ResponsiveKafkaStreams(builder.build(), properties)) {
      streams.setUncaughtExceptionHandler(exception -> {
        caughtExceptions.add(exception);
        return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
      });
      startAppAndAwaitRunning(Duration.ofSeconds(30), streams);

      // When:
      pipeRecords(producer, inputTopic(), inputRecords);

      // Then:
      final long timeout = 2 * ASYNC_SLEEP_DURATION_MS * numInputRecords;
      final boolean allInputProcessed = inputRecordsLatch.await(timeout, TimeUnit.MILLISECONDS);
      if (!allInputProcessed) {
        throw new AssertionError(String.format("Failed to process all %d input records within %dms",
                                 numInputPartitions, timeout));
      }

      final var kvs = readOutput(
          outputTopic(), 0, numInputRecords, numOutputPartitions, false, properties
      );

      final Map<String, String> latestByKey = new HashMap<>();
      for (final var kv : kvs) {
        latestByKey.put((String) kv.key, (String) kv.value);
      }
      for (final String key : keys) {
        final String finalValue = finalOutputRecords.get(key);
        assertThat(latestByKey.get(key), is(finalValue));
      }
    }
    assertThat(caughtExceptions.size(), is(1));
    assertThat(Throwables.getRootCause(caughtExceptions.get(0)),
        instanceOf(InjectedException.class));
    assertThat(latestValues, equalTo(finalOutputRecords));
  }

  @Test
  public void shouldThrowIfStateStoresNotConnectedThroughProcessorSupplier() throws Exception {
    // Given:
    final Map<String, Object> properties = getMutableProperties();

    final StreamsBuilder builder = new StreamsBuilder();
    final KStream<String, InputRecord> input = builder.stream(
        inputTopic(),
        Consumed.with(
            Serdes.String(),
            Serdes.serdeFrom(new InputRecordSerializer(), new InputRecordDeserializer())
        ));

    // this is the old way of connecting StoreBuilders to a topology, which async does not support
    builder.addStateStore(ResponsiveStores.timestampedKeyValueStoreBuilder(
        ResponsiveStores.keyValueStore(ResponsiveKeyValueParams.fact(ASYNC_KV_STORE)),
        Serdes.String(),
        Serdes.String()));

    input.processValues(
        createAsyncProcessorSupplier(
            () -> new FixedKeyProcessor<String, InputRecord, String>() {

              @Override
              public void init(final FixedKeyProcessorContext<String, String> context) {
                // this should throw
                context.getStateStore(ASYNC_KV_STORE);
              }

              @Override
              public void process(final FixedKeyRecord<String, InputRecord> record) {
                // should not get here
                throw new AssertionError("attempted to process something");
              }
            }),

        Named.as("AsyncProcessor"),
        ASYNC_KV_STORE
    );

    final List<Throwable> expectedExceptions = new LinkedList<>();
    final List<Throwable> unexpectedExceptions = new LinkedList<>();

    try (final var streams = new ResponsiveKafkaStreams(builder.build(), properties)) {

      streams.setUncaughtExceptionHandler(exception -> {
        if (Throwables.getRootCause(exception) instanceof IllegalStateException) {
          expectedExceptions.add(exception);
        } else {
          unexpectedExceptions.add(exception);
        }
        return StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
      });

      // When:
      startAppAndAwaitState(State.ERROR, Duration.ofSeconds(15), streams);
    }

    // Then:
    assertThat(unexpectedExceptions.size(), is(0));

    // it's possible for multiple StreamThreads to hit and register the exception before
    // shutdown so just make sure we get at least 1 and no more than the num stream threads
    assertThat(expectedExceptions.size(), greaterThanOrEqualTo(1));
    assertThat(expectedExceptions.size(), lessThanOrEqualTo(STREAMTHREADS_PER_APP));

    for (final var throwable : expectedExceptions) {
      final Throwable rootCause = Throwables.getRootCause(throwable);
      assertThat(
          rootCause.getMessage(),
          equalTo(
              "Processor initialized some stores that were not connected via the "
                  + "ProcessorSupplier, please connect stores for async processors by "
                  + "implementing the ProcessorSupplier#storesNames method"
          ));
    }
  }

  // The "in" processor is a simple counter that just forwards the new count appended to
  // the key and processor name ("IN")
  // The IN count should always match the END count computed downstream
  private SimpleProcessorOutput<String, InputRecord> computeNewValueForSourceProcessor(
      final ValueAndTimestamp<String> oldValAndTimestamp,
      final FixedKeyRecord<String, InputRecord> inputRecord,
      final FixedKeyProcessorContext<String, InputRecord> context
  ) {
    final int newCount = oldValAndTimestamp == null
        ? 1
        : 1 + Integer.parseInt(oldValAndTimestamp.value());

    final String forwardedVal = String.format("%s:%s:%d", inputRecord.key(), "IN", newCount);
    return new SimpleProcessorOutput<>(
        inputRecord.value().withValue(forwardedVal),
        Integer.toString(newCount)
    );
  }

  private String computeNewValueForStatelessAsyncProcessor(
      final FixedKeyRecord<String, InputRecord> inputRecord,
      final FixedKeyProcessorContext<String, String> context
  ) {
    final InputRecord val = inputRecord.value();
    final InjectedFault fault = val.getFault();
    if (fault != null) {
      fault.maybeInject(context.taskId().partition(), context.recordMetadata().get().offset());
    }

    try {
      Thread.sleep(ASYNC_SLEEP_DURATION_MS);
    } catch (final InterruptedException e) {
      throw new RuntimeException("Interrupted during 'RPC' call in async processor", e);
    }
    return inputRecord.value().getValue() + "--PROCESSED";
  }

  // The stateful async processor always forwards the same value that it computes, so over time
  // it appends all records together into one long chain.
  // The values computed and saved/forwarded here will include the upstream IN processor results
  // as a prefix, but won't include the END processor suffix in the async processor results
  // since by definition, that won't be added until the downstream END processor appends
  // its own suffix
  private SimpleProcessorOutput<String, String> computeNewValueForStatefulAsyncProcessor(
      final ValueAndTimestamp<String> oldValAndTimestamp,
      final FixedKeyRecord<String, InputRecord> inputRecord,
      final FixedKeyProcessorContext<String, String> context
  ) {
    final InputRecord val = inputRecord.value();
    final InjectedFault fault = val.getFault();
    if (fault != null) {
      fault.maybeInject(context.taskId().partition(), context.recordMetadata().get().offset());
    }

    if (oldValAndTimestamp == null) {
      return new SimpleProcessorOutput<>(
          inputRecord.value().getValue(),
          inputRecord.value().getValue()
      );
    }

    final String newVal = String.format("%s--%s",
        oldValAndTimestamp.value(),
        inputRecord.value().getValue()
    );

    try {
      Thread.sleep(ASYNC_SLEEP_DURATION_MS);
    } catch (final InterruptedException e) {
      throw new RuntimeException("Interrupted during 'RPC' call in async processor", e);
    }
    return new SimpleProcessorOutput<>(newVal, newVal);
  }

  // The "end"" processor is a slightly-more-advanced counter that just forwards the new count
  // appended to the key and processor name ("END") as well as the input record value
  // The END count should always match the IN count computed upstream
  private SimpleProcessorOutput<String, String> computeNewValueForSinkProcessor(
      final ValueAndTimestamp<String> oldValAndTimestamp,
      final FixedKeyRecord<String, String> inputRecord,
      final FixedKeyProcessorContext<String, String> context
  ) {
    final int newCount = oldValAndTimestamp == null
        ? 1
        : 1 + Integer.parseInt(oldValAndTimestamp.value());

    final String forwardedVal = String.format(
        "%s--%s:%s:%d", inputRecord.value(), inputRecord.key(), "END", newCount
    );
    return new SimpleProcessorOutput<>(forwardedVal, Integer.toString(newCount));
  }

  @SuppressWarnings("checkstyle:linelength")
  private Map<String, Object> getMutableProperties() {
    final Map<String, Object> properties = new HashMap<>(responsiveProps);

    properties.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    properties.put(VALUE_SERIALIZER_CLASS_CONFIG, InputRecordSerializer.class);
    properties.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

    properties.put(APPLICATION_ID_CONFIG, name);
    properties.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
    properties.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
    properties.put(PROCESSING_GUARANTEE_CONFIG, EXACTLY_ONCE_V2);
    properties.put(producerPrefix(TRANSACTION_TIMEOUT_CONFIG), (int) COMMIT_INTERVAL_MS * 2);

    properties.put(ASYNC_THREAD_POOL_SIZE_CONFIG, ASYNC_THREADS_PER_STREAMTHREAD);
    properties.put(NUM_STREAM_THREADS_CONFIG, STREAMTHREADS_PER_APP);

    properties.put(COMMIT_INTERVAL_MS_CONFIG, COMMIT_INTERVAL_MS);
    properties.put(STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);

    properties.put(STORE_FLUSH_RECORDS_TRIGGER_CONFIG, COMMIT_BUFFER_FLUSH_RECORDS);
    properties.put(STORE_FLUSH_INTERVAL_TRIGGER_MS_CONFIG, COMMIT_BUFFER_FLUSH_INTERVAL_MS);

    properties.put(ASYNC_MAX_EVENTS_QUEUED_PER_ASYNC_THREAD_CONFIG, MAX_EVENTS_QUEUED_PER_ASYNC_THREAD);
    properties.put(ASYNC_MAX_EVENTS_QUEUED_PER_KEY_CONFIG, MAX_EVENTS_QUEUED_PER_KEY);
    properties.put(ASYNC_FLUSH_INTERVAL_MS_CONFIG, ASYNC_FLUSH_INTERVAL_MS);

    properties.put(consumerPrefix(ConsumerConfig.METADATA_MAX_AGE_CONFIG), "1000");
    properties.put(consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest");

    return properties;
  }

  private static class InputRecord {
    private final String value;
    private final InjectedFault fault;

    public InputRecord(final String value) {
      this(value, null);
    }

    @JsonCreator
    public InputRecord(
        @JsonProperty("value") final String value,
        @JsonProperty("fault") final InjectedFault fault
    ) {
      this.value = value;
      this.fault = fault;
    }

    public String getValue() {
      return value;
    }

    public InjectedFault getFault() {
      return fault;
    }

    public InputRecord withValue(final String value) {
      return new InputRecord(value, fault);
    }
  }

  public static class InjectedFault {
    private static final Map<InjectKey, Boolean> HISTORY = new ConcurrentHashMap<>();

    private enum Type {
      EXCEPTION
    }

    private enum Frequency {
      ONCE,
      ALWAYS
    }

    private final Type type;
    private final Frequency frequency;

    @JsonCreator
    public InjectedFault(
        @JsonProperty("type") Type type,
        @JsonProperty("frequency") Frequency frequency) {
      this.type = type;
      this.frequency = frequency;
    }

    public Type getType() {
      return type;
    }

    public Frequency getFrequency() {
      return frequency;
    }

    private boolean shouldInject(final int partition, final long offset) {
      if (frequency.equals(Frequency.ALWAYS)) {
        return true;
      }
      final InjectKey k = new InjectKey(partition, offset);
      return HISTORY.put(k, true) == null;
    }

    public void maybeInject(final int partition, final long offset) {
      if (!shouldInject(partition, offset)) {
        return;
      }
      switch (type) {
        case EXCEPTION:
          throw new InjectedException();
        default:
          throw new IllegalStateException();
      }
    }

    private static class InjectKey {
      private final int partition;
      private final long offset;

      public InjectKey(final int partition, final long offset) {
        this.partition = partition;
        this.offset = offset;
      }

      @Override
      public boolean equals(final Object o) {
        if (this == o) {
          return true;
        }
        if (o == null || getClass() != o.getClass()) {
          return false;
        }
        InjectKey injectKey = (InjectKey) o;
        return partition == injectKey.partition && offset == injectKey.offset;
      }

      @Override
      public int hashCode() {
        return Objects.hash(partition, offset);
      }
    }
  }

  public static class InputRecordSerializer implements Serializer<InputRecord> {
    @Override
    public byte[] serialize(String topic, InputRecord data) {
      try {
        return OBJECT_MAPPER.writeValueAsBytes(data);
      } catch (final JsonProcessingException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public static class InputRecordDeserializer implements Deserializer<InputRecord> {
    @Override
    public InputRecord deserialize(String topic, byte[] data) {
      try {
        return OBJECT_MAPPER.readValue(data, InputRecord.class);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public static class InjectedException extends RuntimeException {
    private static final long serialVersionUID = 0L;
  }
}
