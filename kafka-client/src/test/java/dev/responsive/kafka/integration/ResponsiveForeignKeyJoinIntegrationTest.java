package dev.responsive.kafka.integration;

import static dev.responsive.kafka.testutils.IntegrationTestUtils.pipeTimestampedRecords;
import static dev.responsive.kafka.testutils.IntegrationTestUtils.slurpPartition;
import static dev.responsive.kafka.testutils.IntegrationTestUtils.startAppAndAwaitRunning;
import static dev.responsive.kafka.testutils.IntegrationTestUtils.waitTillFullyConsumed;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.TRANSACTION_TIMEOUT_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.COMMIT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.EXACTLY_ONCE_V2;
import static org.apache.kafka.streams.StreamsConfig.NUM_STREAM_THREADS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.PROCESSING_GUARANTEE_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.consumerPrefix;
import static org.apache.kafka.streams.StreamsConfig.producerPrefix;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.responsive.kafka.api.ResponsiveKafkaStreams;
import dev.responsive.kafka.api.config.ResponsiveConfig;
import dev.responsive.kafka.api.config.StorageBackend;
import dev.responsive.kafka.api.stores.ResponsiveDslStoreSuppliers;
import dev.responsive.kafka.testutils.IntegrationTestUtils;
import dev.responsive.kafka.testutils.KeyValueTimestamp;
import dev.responsive.kafka.testutils.ResponsiveConfigParam;
import dev.responsive.kafka.testutils.ResponsiveExtension;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.containers.MongoDBContainer;

public class ResponsiveForeignKeyJoinIntegrationTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final int MAX_POLL_MS = 5000;

  @RegisterExtension
  static ResponsiveExtension EXTENSION = new ResponsiveExtension(StorageBackend.MONGO_DB);

  private static final String INVENTORY_TOPIC = "inventory";
  private static final String MERCHANT_TOPIC = "merchant";
  private static final String OUTPUT_TOPIC = "output";

  private Admin admin;
  private final Map<String, Object> responsiveProps = new HashMap<>();

  private String name;
  private MongoDBContainer mongo;

  @BeforeEach
  public void before(
      final TestInfo info,
      final Admin admin,
      final MongoDBContainer mongo,
      @ResponsiveConfigParam final Map<String, Object> responsiveProps
  ) throws InterruptedException, ExecutionException {
    // add displayName to name to account for parameterized tests
    final var caps = info.getTestMethod().orElseThrow().getName()
        .replaceAll("[a-z]", "");
    name = caps + "-" + new Random().nextInt();
    this.mongo = mongo;
    this.admin = admin;

    this.responsiveProps.putAll(responsiveProps);

    final var result = admin.createTopics(
        List.of(
            new NewTopic(inventoryTopic(), Optional.of(1), Optional.empty()),
            new NewTopic(merchantTopic(), Optional.of(1), Optional.empty()),
            new NewTopic(outputTopic(), Optional.of(1), Optional.empty())
        )
    );
    result.all().get();
  }

  private String inventoryTopic() {
    return name + "." + INVENTORY_TOPIC;
  }

  private String merchantTopic() {
    return name + "." + MERCHANT_TOPIC;
  }

  private String outputTopic() {
    return name + "." + OUTPUT_TOPIC;
  }

  @Test
  public void shouldComputeForeignKeyJoinsCorrectly() throws Exception {
    final StreamsBuilder builder = new StreamsBuilder();
    final KTable<Integer, Inventory> inventory = builder.table(
        inventoryTopic(),
        Consumed.with(Serdes.Integer(), new JsonSerde<>(Inventory.class))
    );
    final KTable<Integer, Merchant> merchant = builder.table(
        merchantTopic(),
        Consumed.with(Serdes.Integer(), new JsonSerde<>(Merchant.class))
    );
    final KTable<Integer, Enriched> enriched = inventory.join(
        merchant,
        Inventory::getMerchantId,
        (i, m) -> new Enriched(i.getId(), m.getId(), i.getName(), m.getName())
    );
    enriched.toStream().to(
        outputTopic(),
        Produced.with(Serdes.Integer(), new JsonSerde<>(Enriched.class))
    );

    final Map<String, Object> baseProperties =
        IntegrationTestUtils.getDefaultMutablePropertiesWithStringSerdes(responsiveProps, name);
    final Map<String, Object> producerProperties = new HashMap<>(baseProperties);
    producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
    producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
    final Map<String, Object> consumerProperties = new HashMap<>(baseProperties);
    consumerProperties.put(KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
    consumerProperties.put(VALUE_DESERIALIZER_CLASS_CONFIG, EnrichedDeserializer.class);
    final Map<String, Object> properties = new HashMap<>(baseProperties);
    properties.putAll(getMutableProperties());
    properties.put(StreamsConfig.DSL_STORE_SUPPLIERS_CLASS_CONFIG,
        ResponsiveDslStoreSuppliers.class);

    final Topology topology = builder.build();
    final List<String> sources = sourcesAndInternalTopics(topology);

    try (
        final KafkaProducer<Integer, Inventory> inventoryProducer
            = new KafkaProducer<>(producerProperties);
        final KafkaProducer<Integer, Merchant> merchantProducer
            = new KafkaProducer<>(producerProperties);
        final ResponsiveKafkaStreams app = new ResponsiveKafkaStreams(topology, properties)
    ) {
      startAppAndAwaitRunning(Duration.ofSeconds(60), app);
      pipeTimestampedRecords(merchantProducer, merchantTopic(), List.of(
          new KeyValueTimestamp<>(100, new Merchant(100, "store0"), 1_000L),
          new KeyValueTimestamp<>(101, new Merchant(101, "store1"), 1_000L),
          new KeyValueTimestamp<>(102, new Merchant(102, "store2"), 1_000L)
      ));
      pipeTimestampedRecords(inventoryProducer, inventoryTopic(), List.of(
          new KeyValueTimestamp<>(0, new Inventory(0, 100, "nut"), 2_000L),
          new KeyValueTimestamp<>(1, new Inventory(1, 101, "bolt"), 2_000L),
          new KeyValueTimestamp<>(2, new Inventory(2, 102, "wheel"), 2_000L),
          new KeyValueTimestamp<>(3, new Inventory(3, 101, "axle"), 2_000L),
          new KeyValueTimestamp<>(4, new Inventory(4, 101, "nail"), 2_000L)
      ));
      waitTillInputsFullyConsumed(sources);
      final var consumed = slurpOutput(consumerProperties);
      final var materialized = materialize(consumed);
      assertThat(materialized, is(Map.of(
          0, new Enriched(0, 100, "nut", "store0"),
          1, new Enriched(1, 101, "bolt", "store1"),
          2, new Enriched(2, 102, "wheel", "store2"),
          3, new Enriched(3, 101, "axle", "store1"),
          4, new Enriched(4, 101, "nail", "store1")
      )));
    }

    // restart and verify new outputs after recovery
    try (
        final KafkaProducer<Integer, Inventory> inventoryProducer
            = new KafkaProducer<>(producerProperties);
        final KafkaProducer<Integer, Merchant> merchantProducer
            = new KafkaProducer<>(producerProperties);
        final ResponsiveKafkaStreams app = new ResponsiveKafkaStreams(builder.build(), properties)
    ) {
      startAppAndAwaitRunning(Duration.ofSeconds(60), app);
      pipeTimestampedRecords(merchantProducer, merchantTopic(), List.of(
          new KeyValueTimestamp<>(101, new Merchant(101, "store1-rename"), 3_000L),
          new KeyValueTimestamp<>(103, new Merchant(103, "store3"), 3_000L)
      ));
      pipeTimestampedRecords(inventoryProducer, inventoryTopic(), List.of(
          new KeyValueTimestamp<>(0, new Inventory(0, 101, "nut"), 4_000L),
          new KeyValueTimestamp<>(5, new Inventory(5, 103, "board"), 4_000L),
          new KeyValueTimestamp<>(6, new Inventory(6, 102, "door"), 4_000L)
      ));
      waitTillInputsFullyConsumed(sources);
      final var consumed = slurpOutput(consumerProperties);
      final var materialized = materialize(consumed);
      assertThat(materialized, is(Map.of(
          0, new Enriched(0, 101, "nut", "store1-rename"),
          1, new Enriched(1, 101, "bolt", "store1-rename"),
          2, new Enriched(2, 102, "wheel", "store2"),
          3, new Enriched(3, 101, "axle", "store1-rename"),
          4, new Enriched(4, 101, "nail", "store1-rename"),
          5, new Enriched(5, 103, "board", "store3"),
          6, new Enriched(6, 102, "door", "store2")
      )));
    }
  }

  private List<String> sourcesAndInternalTopics(final Topology topology) {
    final List<String> topics = topology.describe().subtopologies()
        .stream()
        .flatMap(s -> s.nodes().stream())
        .flatMap(n -> (n instanceof TopologyDescription.Source)
            ? ((TopologyDescription.Source) n).topicSet().stream() : Stream.empty())
        .collect(Collectors.toList());
    final List<String> sources = new LinkedList<>();
    sources.add(inventoryTopic());
    sources.add(merchantTopic());
    topics.stream().filter(t -> t.contains("REGISTRATION")).findFirst()
        .map(t -> name + "-" + t)
        .ifPresent(sources::add);
    topics.stream().filter(t -> t.contains("RESPONSE")).findFirst()
        .map(t -> name + "-" + t)
        .ifPresent(sources::add);
    return List.copyOf(sources);
  }

  private List<ConsumerRecord<Integer, Enriched>> slurpOutput(
      final Map<String, Object> properties
  ) {
    return slurpPartition(new TopicPartition(outputTopic(), 0), properties);
  }

  private Map<Integer, Enriched> materialize(
      final List<ConsumerRecord<Integer, Enriched>> records
  ) {
    final var materialized = new HashMap<Integer, Enriched>();
    records.forEach(r -> materialized.put(r.key(), r.value()));
    return Map.copyOf(materialized);
  }

  private void waitTillInputsFullyConsumed(final List<String> sources)
      throws InterruptedException, ExecutionException, TimeoutException {
    for (final var source : sources) {
      waitTillFullyConsumed(
          admin,
          new TopicPartition(source, 0),
          name,
          Duration.ofMinutes(5)
      );
    }
  }

  private Map<String, Object> getMutableProperties() {
    final Map<String, Object> properties = new HashMap<>(responsiveProps);
    properties.put(APPLICATION_ID_CONFIG, name);
    properties.put(PROCESSING_GUARANTEE_CONFIG, EXACTLY_ONCE_V2);
    properties.put(NUM_STREAM_THREADS_CONFIG, 1);
    properties.put(STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
    properties.put(ResponsiveConfig.STORE_FLUSH_RECORDS_TRIGGER_CONFIG, 1);
    // this ensures we can control the commits by explicitly requesting a commit
    properties.put(COMMIT_INTERVAL_MS_CONFIG, 2000);
    properties.put(producerPrefix(TRANSACTION_TIMEOUT_CONFIG), 20_000);
    properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
    properties.put(consumerPrefix(ConsumerConfig.METADATA_MAX_AGE_CONFIG), "1000");
    properties.put(consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest");
    properties.put(consumerPrefix(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG), MAX_POLL_MS);
    properties.put(consumerPrefix(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG), MAX_POLL_MS);
    properties.put(consumerPrefix(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG), MAX_POLL_MS - 1);
    return properties;
  }

  public static class JsonSerializer<T> implements Serializer<T> {
    public JsonSerializer() {
    }

    @Override
    public byte[] serialize(String s, T t) {
      try {
        return MAPPER.writeValueAsBytes(t);
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public static class JsonDeserializer<T> implements Deserializer<T> {
    private final Class<T> clazz;

    public JsonDeserializer(Class<T> clazz) {
      this.clazz = clazz;
    }

    @Override
    public T deserialize(String s, byte[] bytes) {
      try {
        return MAPPER.readValue(bytes, clazz);
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public static class JsonSerde<T> implements Serde<T> {
    private JsonDeserializer<T> deserializer;

    public JsonSerde(final Class<T> clazz) {
      deserializer = new JsonDeserializer<>(clazz);
    }

    @Override
    public Serializer<T> serializer() {
      return new JsonSerializer<>();
    }

    @Override
    public Deserializer<T> deserializer() {
      return deserializer;
    }
  }

  public static class EnrichedDeserializer extends JsonDeserializer<Enriched> {
    public EnrichedDeserializer() {
      super(Enriched.class);
    }
  }

  private static class Inventory {
    private final int id;
    private final int merchantId;
    private final String name;

    @JsonCreator
    public Inventory(
        @JsonProperty("id") final int id,
        @JsonProperty("merchantId") final int merchantId,
        @JsonProperty("name") final String name
    ) {
      this.id = id;
      this.merchantId = merchantId;
      this.name = name;
    }

    public int getId() {
      return id;
    }

    public int getMerchantId() {
      return merchantId;
    }

    public String getName() {
      return name;
    }
  }

  private static class Merchant {
    private final int id;
    private final String name;

    @JsonCreator
    private Merchant(@JsonProperty("id") final int id, @JsonProperty("name") final String name) {
      this.id = id;
      this.name = name;
    }

    public int getId() {
      return id;
    }

    public String getName() {
      return name;
    }
  }

  private static class Enriched {
    private final int id;
    private final int merchantId;
    private final String name;
    private final String merchantName;

    @JsonCreator
    public Enriched(
        @JsonProperty("id") final int id,
        @JsonProperty("merchantId") final int merchantId,
        @JsonProperty("name") final String name,
        @JsonProperty("merchantName") final String merchantName
    ) {
      this.id = id;
      this.merchantId = merchantId;
      this.name = name;
      this.merchantName = merchantName;
    }

    public int getId() {
      return id;
    }

    public int getMerchantId() {
      return merchantId;
    }

    public String getName() {
      return name;
    }

    public String getMerchantName() {
      return merchantName;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final Enriched enriched = (Enriched) o;
      return id == enriched.id
          && merchantId == enriched.merchantId
          && Objects.equals(name, enriched.name)
          && Objects.equals(merchantName, enriched.merchantName);
    }

    @Override
    public int hashCode() {
      return Objects.hash(id, merchantId, name, merchantName);
    }

    @Override
    public String toString() {
      return "Enriched{"
          + "id=" + id
          + ", merchantId=" + merchantId
          + ", name='" + name + '\''
          + ", merchantName='" + merchantName + '\''
          + '}';
    }
  }
}
