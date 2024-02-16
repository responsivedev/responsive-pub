package dev.responsive.kafka.integration;

import static dev.responsive.kafka.testutils.IntegrationTestUtils.pipeRecords;
import static dev.responsive.kafka.testutils.IntegrationTestUtils.startAppAndAwaitRunning;
import static java.util.Arrays.asList;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_RECORDS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_SERVER_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.COMMIT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.NUM_STREAM_THREADS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.REQUEST_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.consumerPrefix;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import dev.responsive.kafka.api.ResponsiveKafkaStreams;
import dev.responsive.kafka.api.config.ResponsiveConfig;
import dev.responsive.kafka.api.config.StorageBackend;
import dev.responsive.kafka.api.stores.ResponsiveStores;
import dev.responsive.kafka.api.stores.ResponsiveWindowParams;
import dev.responsive.kafka.testutils.IntegrationTestUtils;
import dev.responsive.kafka.testutils.KeyValueTimestamp;
import dev.responsive.kafka.testutils.ResponsiveConfigParam;
import dev.responsive.kafka.testutils.ResponsiveExtension;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.RegisterExtension;

public class ResponsiveSessionStoreIntegrationTest {

  @RegisterExtension
  static ResponsiveExtension EXTENSION = new ResponsiveExtension(StorageBackend.MONGO_DB);

  private static final String INPUT_TOPIC = "input";
  private static final String OTHER_TOPIC = "other";
  private static final String OUTPUT_TOPIC = "output";

  private final Map<String, Object> responsiveProps = new HashMap<>();

  private String name;
  private Admin admin;

  @BeforeEach
  public void before(
      final TestInfo info,
      final Admin admin,
      @ResponsiveConfigParam final Map<String, Object> responsiveProps
  ) throws InterruptedException, ExecutionException {
    // append a random int to avoid naming collisions on repeat runs
    name = info.getTestMethod().orElseThrow().getName() + "-" + new Random().nextInt();

    this.responsiveProps.putAll(responsiveProps);
    this.responsiveProps.put(ResponsiveConfig.WINDOW_BLOOM_FILTER_COUNT_CONFIG, 1);
    this.responsiveProps.put(ResponsiveConfig.WINDOW_BLOOM_FILTER_EXPECTED_KEYS_CONFIG, 10);

    this.admin = admin;
    final var result = admin.createTopics(
        List.of(
            new NewTopic(inputTopic(), Optional.of(1), Optional.empty()),
            new NewTopic(otherTopic(), Optional.of(1), Optional.empty()),
            new NewTopic(outputTopic(), Optional.of(1), Optional.empty())
        )
    );
    result.all().get();
  }

  @Test
  public void shouldComputeSessionWindowAggregate() throws Exception {
    // Given:
    final Map<String, Object> properties = getMutablePropertiesWithStringSerdes();

    final StreamsBuilder builder = new StreamsBuilder();

    final ConcurrentMap<Windowed<String>, String> results = new ConcurrentHashMap<>();

    final Duration inactivityGap = Duration.ofSeconds(5);
    final Duration gracePeriod = Duration.ofSeconds(2);

    // Start from timestamp of 0L to get predictable results
    final List<KeyValueTimestamp<String, String>> input1 = asList(
        new KeyValueTimestamp<>("key", "a", 0L),
        new KeyValueTimestamp<>("key", "b", 1_000L),
        new KeyValueTimestamp<>("key", "c", 6_000L)
    );
    final CountDownLatch outputLatch = new CountDownLatch(input1.size());

    final KStream<String, String> input = builder.stream(inputTopic());
    input
        .groupByKey()
        .windowedBy(SessionWindows.ofInactivityGapAndGrace(inactivityGap, gracePeriod))
        .aggregate(
            () -> "",
            (k, v, agg) -> agg + v,
            (aggKey, agg1, agg2) -> agg1 + agg2,
            ResponsiveStores.sessionMaterialized(
                ResponsiveWindowParams.window(
                    name,
                    inactivityGap,
                    gracePeriod
                )
            )
        )
        .toStream()
        .peek((k, v) -> {
          results.put(k, v);
          outputLatch.countDown();
        })
        // discard the window, so we don't have to serialize it
        // we're not checking the output topic anyway
        .selectKey((k, v) -> k.key())
        .to(outputTopic());

    // When:
    properties.put(APPLICATION_SERVER_CONFIG, "host1:1024");
    final KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
    try (
        final ResponsiveKafkaStreams kafkaStreams =
            new ResponsiveKafkaStreams(builder.build(), properties);
    ) {
      startAppAndAwaitRunning(Duration.ofSeconds(15), kafkaStreams);

      pipeRecords(producer, inputTopic(), input1);
      outputLatch.await();

      assertThat(results.size(), equalTo(4));
      assertThat(results, Matchers.hasEntry(windowedKey(0L), "abc"));    // [0, 5s]
    }
  }

  private Windowed<String> windowedKey(final long startMs) {
    return new Windowed<>("key", new TimeWindow(startMs, startMs + 10_000));
  }

  private Map<String, Object> getMutablePropertiesWithStringSerdes() {
    final Map<String, Object> properties = getMutableProperties();
    properties.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    properties.put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    properties.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    properties.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
    properties.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
    return properties;
  }

  private Map<String, Object> getMutableProperties() {
    final Map<String, Object> properties = new HashMap<>(responsiveProps);

    properties.put(KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
    properties.put(VALUE_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
    properties.put(KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
    properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);

    properties.put(APPLICATION_ID_CONFIG, name);
    properties.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.LongSerde.class.getName());
    properties.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.LongSerde.class.getName());
    properties.put(NUM_STREAM_THREADS_CONFIG, 1);
    properties.put(COMMIT_INTERVAL_MS_CONFIG, 1); // commit as often as possible

    properties.put(consumerPrefix(REQUEST_TIMEOUT_MS_CONFIG), 5_000);
    properties.put(consumerPrefix(SESSION_TIMEOUT_MS_CONFIG), 5_000 - 1);

    properties.put(consumerPrefix(MAX_POLL_RECORDS_CONFIG), 1);

    properties.put(ResponsiveConfig.STORE_FLUSH_RECORDS_TRIGGER_CONFIG, 1);

    return properties;
  }

  private String inputTopic() {
    return name + "." + INPUT_TOPIC;
  }

  private String outputTopic() {
    return name + "." + OUTPUT_TOPIC;
  }

  private String otherTopic() {
    return name + "." + OTHER_TOPIC;
  }
}
