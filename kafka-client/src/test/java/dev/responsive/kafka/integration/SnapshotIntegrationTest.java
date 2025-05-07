package dev.responsive.kafka.integration;

import static dev.responsive.kafka.testutils.IntegrationTestUtils.createTopicsAndWait;
import static dev.responsive.kafka.testutils.IntegrationTestUtils.pipeInput;
import static dev.responsive.kafka.testutils.IntegrationTestUtils.readOutput;
import static dev.responsive.kafka.testutils.IntegrationTestUtils.startAppAndAwaitRunning;
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
import static org.hamcrest.Matchers.is;

import dev.responsive.kafka.api.ResponsiveKafkaStreams;
import dev.responsive.kafka.api.config.ResponsiveConfig;
import dev.responsive.kafka.api.config.StorageBackend;
import dev.responsive.kafka.internal.db.rs3.RS3ReadOnlyKeyValueLogicalStore;
import dev.responsive.kafka.internal.db.rs3.client.jni.JNIRs3ReaderClient;
import dev.responsive.kafka.internal.snapshot.LocalSnapshotApi;
import dev.responsive.kafka.internal.snapshot.ReadOnlyKeyValueLogicalStore;
import dev.responsive.kafka.internal.snapshot.Snapshot;
import dev.responsive.kafka.internal.snapshot.SnapshotApi;
import dev.responsive.kafka.internal.snapshot.SnapshotPartitionPoint;
import dev.responsive.kafka.internal.snapshot.SnapshotSupport;
import dev.responsive.kafka.internal.snapshot.topic.TopicSnapshotStore;
import dev.responsive.kafka.testutils.ResponsiveConfigParam;
import dev.responsive.kafka.testutils.ResponsiveExtension;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SnapshotIntegrationTest {
  private static final Logger LOG = LoggerFactory.getLogger(SnapshotIntegrationTest.class);

  @RegisterExtension
  static ResponsiveExtension EXTENSION = new ResponsiveExtension(StorageBackend.RS3);

  private static final int MAX_POLL_MS = 5000;
  private static final String INPUT_TOPIC = "input";
  private static final String OUTPUT_TOPIC = "output";

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
    name = info.getTestMethod()
        .orElseThrow(() -> new IllegalStateException("must have test method"))
        .getName();

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
  public void shouldTakeSnapshotsAndReadSnapshottedState()
      throws Exception {
    assertEnvironmentVars();
    final Map<String, Object> properties = getMutableProperties();
    final KafkaProducer<Long, Long> producer = new KafkaProducer<>(properties);

    try (
        final ResponsiveKafkaStreams streams = buildStreams(properties);
        final SnapshotApi api = new LocalSnapshotApi(TopicSnapshotStore.create(properties))
    ) {
      startAppAndAwaitRunning(Duration.ofSeconds(10), streams);
      final List<Snapshot> completedSnapshots = new LinkedList<>();

      for (int i = 0; i < 5; i++) {
        // write some values to the source
        pipeInput(inputTopic(), 2, producer, System::currentTimeMillis, 0, 15, 0, 1, 2, 3);
        readOutput(outputTopic(), 0, 30, false, properties);

        // take a snapshot
        final Snapshot snapshot = api.createSnapshot();
        final Snapshot completed = awaitCompletedSnapshot(
            snapshot.generation(),
            api,
            producer,
            Duration.ofSeconds(60)
        );
        completedSnapshots.add(completed);

        validateSnapshots(completedSnapshots);
      }
      try (final JNIRs3ReaderClient rc = JNIRs3ReaderClient.withConfig(loadReaderConfig())) {
        final Function<Snapshot, ReadOnlyKeyValueLogicalStore<Long, Long>> storeFactory =
            snapshot -> RS3ReadOnlyKeyValueLogicalStore.createFromSnapshot(
                rc,
                snapshot,
                "count-store",
                Serdes.Long(),
                Serdes.Long(),
                true,
                (k, p) -> (int) (new LongDeserializer().deserialize("", k.get()) % p)
            );
        final Optional<Long> partitionPoint = SnapshotPartitionPoint.partitionPoint(
            api,
            storeFactory,
            store -> store.get(1L) > 30
        );
        assertThat(partitionPoint, is(Optional.of(3L)));
        SnapshotPartitionPoint.printPartitionPoint(
            api,
            storeFactory,
            store -> store.get(1L) > 30
        );
      }
    }
  }

  private void validateSnapshots(final List<Snapshot> completedSnapshots) {
    for (int i = 0; i < completedSnapshots.size(); i++) {
      validateSnapshot(completedSnapshots.get(i), i + 1);
    }
  }

  private void validateSnapshot(final Snapshot snapshot, int iteration) {
    final Deserializer<Long> deserializer = new LongDeserializer();
    try (
        final JNIRs3ReaderClient readerClient = JNIRs3ReaderClient.withConfig(loadReaderConfig());
        final RS3ReadOnlyKeyValueLogicalStore<Long, Long> store
            = RS3ReadOnlyKeyValueLogicalStore.createFromSnapshot(
                readerClient,
                snapshot,
                "count-store",
                Serdes.Long(),
                Serdes.Long(),
                true,
                (k, p) -> (int) (deserializer.deserialize("", k.get()) % p)
        )
    ) {
      final var iter = store.range(0L, 3L);
      while (iter.hasNext()) {
        final var v = iter.next();
        System.out.printf(
            "RDT: %d -> %s%n",
            v.key,
            v.value
        );
        assertThat(v.value, is(iteration * 15L));
      }
      iter.close();
    }
  }

  private void assertEnvironmentVars() {
    assertThat(
        "Must set " + ResponsiveExtension.RS3_ALLOW_S3_HTTP + " to true",
        System.getenv(ResponsiveExtension.RS3_ALLOW_S3_HTTP),
        is("true")
    );
  }

  private Snapshot awaitCompletedSnapshot(
      final long generation,
      final SnapshotApi api,
      final KafkaProducer<Long, Long> producer,
      final Duration timeout
  ) throws InterruptedException {
    final Instant start = Instant.now();
    while (Duration.between(start, Instant.now()).compareTo(timeout) < 0) {
      // write some data to an alternate keyspace to churn the application
      // this is needed until we support snapshots of idle applications
      pipeInput(inputTopic(), 2, producer, System::currentTimeMillis, 0, 5, 4, 5);
      final var current = api.getCurrentSnapshot();
      if (current.generation() == generation && !current.state().equals(Snapshot.State.CREATED)) {
        return current;
      }
      Thread.sleep(100);
    }
    throw new RuntimeException("snapshot failed to complete before timeout");
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
    properties.put(PROCESSING_GUARANTEE_CONFIG, EXACTLY_ONCE_V2);
    properties.put(STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
    properties.put(NUM_STREAM_THREADS_CONFIG, 2);
    properties.put(ResponsiveConfig.SNAPSHOTS_CONFIG, SnapshotSupport.LOCAL.name());
    properties.put(ResponsiveConfig.SNAPSHOTS_LOCAL_STORE_TOPIC_REPLICATION_FACTOR, "1");
    properties.put(ResponsiveConfig.RS3_TLS_ENABLED_CONFIG, false);

    // this ensures we can control the commits by explicitly requesting a commit
    properties.put(COMMIT_INTERVAL_MS_CONFIG, 100);
    properties.put(producerPrefix(TRANSACTION_TIMEOUT_CONFIG), 20_000);

    properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
    properties.put(consumerPrefix(ConsumerConfig.METADATA_MAX_AGE_CONFIG), "1000");
    properties.put(consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest");
    properties.put(consumerPrefix(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG), MAX_POLL_MS);
    properties.put(consumerPrefix(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG), MAX_POLL_MS);
    properties.put(consumerPrefix(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG), MAX_POLL_MS - 1);

    return properties;
  }

  private ResponsiveKafkaStreams buildStreams(
      final Map<String, Object> originals
  ) {
    final Map<String, Object> properties = new HashMap<>(originals);
    final StreamsBuilder builder = new StreamsBuilder();

    final KStream<Long, Long> input = builder.stream(inputTopic());
    input
        .groupByKey()
        .count(Named.as("count"), Materialized.as("count-store"))
        .toStream(Named.as("counts-stream"))
        .to(outputTopic());

    return new ResponsiveKafkaStreams(builder.build(), properties);
  }

  private String loadReaderConfig() {
    final byte[] tmplBytes = slurp("/rs3readconfig.toml");
    final String tmpl = new String(tmplBytes, Charset.defaultCharset());
    return tmpl.replace("S3_ENDPOINT", ResponsiveExtension.minio.getS3URL());
  }

  private byte[] slurp(final String path) {
    try (final InputStream s = SnapshotIntegrationTest.class.getResourceAsStream(path)) {
      if (s == null) {
        throw new RuntimeException("could not find " + path);
      }
      return s.readAllBytes();
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }
}