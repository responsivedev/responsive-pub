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
import static dev.responsive.kafka.api.config.ResponsiveConfig.STORE_FLUSH_RECORDS_TRIGGER_CONFIG;
import static dev.responsive.kafka.testutils.IntegrationTestUtils.createTopicsAndWait;
import static dev.responsive.kafka.testutils.IntegrationTestUtils.pipeRecords;
import static dev.responsive.kafka.testutils.IntegrationTestUtils.readOutput;
import static dev.responsive.kafka.testutils.IntegrationTestUtils.startAppAndAwaitRunning;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.responsive.kafka.api.ResponsiveKafkaStreams;
import dev.responsive.kafka.api.config.ResponsiveConfig;
import dev.responsive.kafka.api.config.StorageBackend;
import dev.responsive.kafka.api.stores.ResponsiveKeyValueParams;
import dev.responsive.kafka.api.stores.ResponsiveStores;
import dev.responsive.kafka.testutils.ResponsiveConfigParam;
import dev.responsive.kafka.testutils.ResponsiveExtension;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
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

  private static final String INPUT_TOPIC = "input";
  private static final String OUTPUT_TOPIC = "output";
  private static final String ASYNC_KV_STORE = "async-kv";

  private final Map<String, Object> responsiveProps = new HashMap<>();

  private String name;
  private Admin admin;

  @BeforeEach
  public void before(
      final TestInfo info,
      final Admin admin,
      @ResponsiveConfigParam final Map<String, Object> responsiveProps
  ) {
    // add displayName to name to account for parameterized tests
    name = info.getDisplayName().replace("()", "");

    this.responsiveProps.putAll(responsiveProps);

    this.admin = admin;
    createTopicsAndWait(admin, Map.of(inputTopic(), 10, outputTopic(), 1));
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
  public void shouldProcessEventsInOrderByKey() throws Exception {
    // Given:
    final List<String> keys = List.of("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k",
                                      "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v",
                                      "w", "x", "y", "z");

    // produce 5 records for each key, with keys interleaved across iterations
    final List<KeyValue<String, InputRecord>> inputRecords = new LinkedList<>();
    for (int val = 1; val < 6; ++val) {
      for (final String key : keys) {
        final InputRecord inputRecord;
        if (val == 2 && key.equals("b")) {
          inputRecord = new InputRecord(key + val, new InjectedFault(
              InjectedFault.Type.EXCEPTION,
              new RuntimeException("oops"),
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
      final String finalValue = String.format("%s1%s2%s3%s4%s5", key, key, key, key, key);
      finalOutputRecords.put(key, finalValue);
    }

    final Map<String, Object> properties = getMutableProperties();
    final KafkaProducer<String, InputRecord> producer = new KafkaProducer<>(properties);
    
    final Map<String, String> latestValues = new ConcurrentHashMap<>();

    final StreamsBuilder builder = new StreamsBuilder();

    final KStream<String, InputRecord> input = builder.stream(
        inputTopic(),
        Consumed.with(Serdes.String(), Serdes.serdeFrom(new InputRecordSerializer(), new InputRecordDeserializer())));
    input
        .processValues(
            createAsyncProcessorSupplier(new UserFixedKeyProcessorSupplier(latestValues)),
            Named.as("AsyncProcessor"),
            ASYNC_KV_STORE)
        .to(outputTopic());

    // The total number of records processed, equal to the total number of output records
    // ONLY when caching is disabled
    final int numProcessedRecords = keys.size() * 5;

    List<Throwable> caughtExceptions = new LinkedList<>();
    try (final var streams = new ResponsiveKafkaStreams(builder.build(), properties)) {
      streams.setUncaughtExceptionHandler(exception -> {
        caughtExceptions.add(exception);
        return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
      });
      startAppAndAwaitRunning(Duration.ofSeconds(10), streams);

      // When:
      pipeRecords(producer, inputTopic(), inputRecords);

      // Then:
      final var kvs = readOutput(outputTopic(), 0, numProcessedRecords, true, properties);

      for (final String key : keys) {
        final String finalValue = finalOutputRecords.get(key);
        assertThat(kvs, hasItem(new KeyValue<>(key, finalValue)));
      }
    }
    assertThat(caughtExceptions.size(), is(1));
    assertThat(latestValues, equalTo(finalOutputRecords));
  }

  private static class InputRecord {
    private final String value;
    private final InjectedFault fault;

    public InputRecord(final String value) {
      this(value, null);
    }

    @JsonCreator
    public InputRecord(@JsonProperty("value") final String value, @JsonProperty("fault") final InjectedFault fault) {
      this.value = value;
      this.fault = fault;
    }

    public String getValue() {
      return value;
    }

    public InjectedFault getFault() {
      return fault;
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
    private final RuntimeException exception;
    private final Frequency frequency;

    @JsonCreator
    public InjectedFault(
        @JsonProperty("type") Type type,
        @JsonProperty("exception") RuntimeException exception,
        @JsonProperty("frequency") Frequency frequency) {
      this.type = type;
      this.exception = exception;
      this.frequency = frequency;
    }

    public Type getType() {
      return type;
    }

    public Exception getException() {
      return exception;
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
          throw exception;
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
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
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

  private static class UserFixedKeyProcessor implements FixedKeyProcessor<String, InputRecord, String> {
    
    private final Map<String, String> latestValues;

    private final String streamThreadName;
    
    private int partition;
    private FixedKeyProcessorContext<String, String> context;
    private TimestampedKeyValueStore<String, String> kvStore;

    public UserFixedKeyProcessor(
        final Map<String, String> latestValues
    ) {
      this.latestValues = latestValues;
      this.streamThreadName = Thread.currentThread().getName();
    }

    @Override
    public void init(final FixedKeyProcessorContext<String, String> context) {
      this.context = context;
      this.kvStore = context.getStateStore(ASYNC_KV_STORE);
      this.partition = context.taskId().partition();

      System.out.printf("stream-thread [%s][%s] Initialized processor%n",
                        streamThreadName, partition
      );
    }

    @Override
    public void process(final FixedKeyRecord<String, InputRecord> record) {
      final InputRecord val = record.value();
      System.out.printf("stream-thread [%s][%d] Processing record: <%s, %s>%n",
                        streamThreadName, partition, record.key(), val
      );

      final InjectedFault fault = val.getFault();
      if (fault != null) {
        fault.maybeInject(partition, context.recordMetadata().get().offset());
      }
      
      final ValueAndTimestamp<String> oldValAndTimestamp = kvStore.get(record.key());
      final String newVal = oldValAndTimestamp == null
          ? val.getValue()
          : oldValAndTimestamp.value() + val.getValue();

      kvStore.put(record.key(), ValueAndTimestamp.make(newVal, record.timestamp()));
      context.forward(record.withValue(newVal));
      
      latestValues.put(record.key(), newVal);
    }
    
    @Override
    public void close() {
      System.out.printf("stream-thread [%s][%s] Closed processor%n",
                        streamThreadName, partition
      );
    }
  }
  
  private static class UserFixedKeyProcessorSupplier 
      implements FixedKeyProcessorSupplier<String, InputRecord, String> {

    private final Map<String, String> latestValues;

    public UserFixedKeyProcessorSupplier(
        final Map<String, String> latestValues
    ) {
      this.latestValues = latestValues;
    }

    @Override
    public FixedKeyProcessor<String, InputRecord, String> get() {
      return new UserFixedKeyProcessor(latestValues);
    }

    @Override
    public Set<StoreBuilder<?>> stores() {
      return Collections.singleton(ResponsiveStores.timestampedKeyValueStoreBuilder(
          ResponsiveStores.keyValueStore(ResponsiveKeyValueParams.fact(ASYNC_KV_STORE)),
          Serdes.String(),
          Serdes.String()
      ));
    }
  }

  private Map<String, Object> getMutableProperties() {
    final Map<String, Object> properties = new HashMap<>(responsiveProps);
    
    properties.put(ResponsiveConfig.ASYNC_THREAD_POOL_SIZE_CONFIG, 5);
    properties.put(NUM_STREAM_THREADS_CONFIG, 5);

    properties.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    properties.put(VALUE_SERIALIZER_CLASS_CONFIG, InputRecordSerializer.class);
    properties.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

    properties.put(APPLICATION_ID_CONFIG, name);
    properties.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
    properties.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
    properties.put(STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
    properties.put(STORE_FLUSH_RECORDS_TRIGGER_CONFIG, 1);
    properties.put(COMMIT_INTERVAL_MS_CONFIG, 1);
    properties.put(PROCESSING_GUARANTEE_CONFIG, EXACTLY_ONCE_V2);

    properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
    properties.put(consumerPrefix(ConsumerConfig.METADATA_MAX_AGE_CONFIG), "1000");
    properties.put(consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest");

    return properties;
  }

}
