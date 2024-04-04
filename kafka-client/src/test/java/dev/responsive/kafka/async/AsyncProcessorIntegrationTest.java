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
import static org.apache.kafka.streams.StreamsConfig.NUM_STREAM_THREADS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.consumerPrefix;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;

import dev.responsive.kafka.api.ResponsiveKafkaStreams;
import dev.responsive.kafka.api.config.ResponsiveConfig;
import dev.responsive.kafka.api.config.StorageBackend;
import dev.responsive.kafka.api.stores.ResponsiveKeyValueParams;
import dev.responsive.kafka.api.stores.ResponsiveStores;
import dev.responsive.kafka.testutils.ResponsiveConfigParam;
import dev.responsive.kafka.testutils.ResponsiveExtension;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.RegisterExtension;

public class AsyncProcessorIntegrationTest {

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
    createTopicsAndWait(admin, Map.of(inputTopic(), 2, outputTopic(), 1));
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
    final Map<String, Object> properties = getMutableProperties();
    final KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
    
    final AtomicInteger processed = new AtomicInteger(0);
    final Map<String, String> latestValues = new ConcurrentHashMap<>();

    final StreamsBuilder builder = new StreamsBuilder();

    final KStream<String, String> input = builder.stream(inputTopic());
    input
        .processValues(
            createAsyncProcessorSupplier(
                new UserFixedKeyProcessorSupplier(processed, latestValues)),
            Named.as("AsyncProcessor"),
            ASYNC_KV_STORE)
        .to(outputTopic());

    try (final var streams = new ResponsiveKafkaStreams(builder.build(), properties)) {
      startAppAndAwaitRunning(Duration.ofSeconds(10), streams);

      final List<KeyValue<String, String>> inputRecords = new LinkedList<>();

      final List<String> keys = List.of("A", "B", "C", "D", "E");
      int value = 0;
      for (final String key : keys) {
        // produce 10 records for each key, with the value increasing by 1 for each new record
        for (int i = 0; i < 10; ++i) {
          inputRecords.add(new KeyValue<>(key, Integer.toString(value)));
        }
      }

      // When:
      pipeRecords(producer, inputTopic(), inputRecords);

      // Then:
      final var kvs = readOutput(outputTopic(), 0, 50, true, properties);
      assertThat(
          kvs,
          hasItems(
              new KeyValue<>("A", "123456789"),
              new KeyValue<>("B", "10111213141516171819"),
              new KeyValue<>("C", "20212223242526272829"),
              new KeyValue<>("D", "30313233343536373839"),
              new KeyValue<>("E", "40414243444546474849"))
      );

    }
    assertThat(processed.get(), equalTo(50));
  }
  
  private static class UserFixedKeyProcessor implements FixedKeyProcessor<String, String, String> {
    
    private final AtomicInteger processed;
    private final Map<String, String> latestValues;
    
    private final String streamThreadName;
    
    private int partition;
    private FixedKeyProcessorContext<String, String> context;
    private KeyValueStore<String, String> kvStore;

    public UserFixedKeyProcessor(
        final AtomicInteger processed,
        final Map<String, String> latestValues
    ) {
      this.processed = processed;
      this.latestValues = latestValues;
      this.streamThreadName = Thread.currentThread().getName();
    }

    @Override
    public void init(final FixedKeyProcessorContext<String, String> context) {
      this.context = context;
      this.kvStore = context.getStateStore(ASYNC_KV_STORE);
      this.partition = context.taskId().partition();

      System.out.printf("stream-thread [%s][%s] Initialized processor",
                        streamThreadName, partition
      );
    }

    @Override
    public void process(final FixedKeyRecord<String, String> record) {
      System.out.printf("stream-thread [%s][%d] Processing record: <%s, %s>",
                        streamThreadName, partition, record.key(), record.value()
      );
      
      final String val = kvStore.get(record.key());
      final String newVal = val + record.value();
      
      kvStore.put(record.key(), newVal);
      context.forward(record.withValue(newVal));
      
      processed.incrementAndGet();
      latestValues.put(record.key(), newVal);
    }
    
    @Override
    public void close() {
      System.out.printf("stream-thread [%s][%s] Closed processor",
                        streamThreadName, partition
      );
    }
    
  }
  
  private static class UserFixedKeyProcessorSupplier 
      implements FixedKeyProcessorSupplier<String, String, String> {

    private final AtomicInteger processed;
    private final Map<String, String> latestValues;

    public UserFixedKeyProcessorSupplier(
        final AtomicInteger processed,
        final Map<String, String> latestValues
    ) {
      this.processed = processed;
      this.latestValues = latestValues;
    }

    @Override
    public FixedKeyProcessor<String, String, String> get() {
      return new UserFixedKeyProcessor(processed, latestValues);
    }

    @Override
    public Set<StoreBuilder<?>> stores() {
      return Collections.singleton(ResponsiveStores.timestampedKeyValueStoreBuilder(
          ResponsiveStores.keyValueStore(ResponsiveKeyValueParams.keyValue(ASYNC_KV_STORE)),
          Serdes.String(),
          Serdes.String()
      ));
    }
    
  }

  private Map<String, Object> getMutableProperties() {
    final Map<String, Object> properties = new HashMap<>(responsiveProps);
    
    properties.put(ResponsiveConfig.ASYNC_THREAD_POOL_SIZE_CONFIG, 5);

    properties.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    properties.put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    properties.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

    properties.put(APPLICATION_ID_CONFIG, name);
    properties.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
    properties.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
    properties.put(NUM_STREAM_THREADS_CONFIG, 1);
    properties.put(STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
    properties.put(STORE_FLUSH_RECORDS_TRIGGER_CONFIG, 1);
    properties.put(COMMIT_INTERVAL_MS_CONFIG, 1);

    properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
    properties.put(consumerPrefix(ConsumerConfig.METADATA_MAX_AGE_CONFIG), "1000");
    properties.put(consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest");

    return properties;
  }

}
