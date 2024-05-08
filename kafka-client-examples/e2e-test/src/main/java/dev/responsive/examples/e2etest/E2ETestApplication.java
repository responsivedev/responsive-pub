package dev.responsive.examples.e2etest;

import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.REQUEST_TIMEOUT;
import static dev.responsive.kafka.api.config.ResponsiveConfig.CASSANDRA_HOSTNAME_CONFIG;
import static dev.responsive.kafka.api.config.ResponsiveConfig.CASSANDRA_PORT_CONFIG;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.CreateKeyspace;
import dev.responsive.examples.e2etest.Schema.InputRecord;
import dev.responsive.examples.e2etest.Schema.OutputRecord;
import dev.responsive.kafka.api.ResponsiveKafkaStreams;
import dev.responsive.kafka.api.async.AsyncFixedKeyProcessorSupplier;
import dev.responsive.kafka.api.config.ResponsiveConfig;
import dev.responsive.kafka.api.stores.ResponsiveKeyValueParams;
import dev.responsive.kafka.api.stores.ResponsiveStores;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class E2ETestApplication {
  private static final Logger LOG = LoggerFactory.getLogger(E2ETestApplication.class);

  private KafkaStreams kafkaStreams;
  private final Map<String, Object> properties;
  private boolean stopped = false;
  private final String name;
  private final String inputTopic;
  private final String outputTopic;
  private final int partitions;
  private final int exceptionThreshold;

  public E2ETestApplication(
      final Map<String, Object> properties,
      final String name,
      final String inputTopic,
      final String outputTopic,
      final int partitions,
      final int exceptionThreshold
  ) {
    this.properties = Objects.requireNonNull(properties);
    this.inputTopic = Objects.requireNonNull(inputTopic);
    this.outputTopic = Objects.requireNonNull(outputTopic);
    this.name = Objects.requireNonNull(name);
    this.partitions = partitions;
    this.exceptionThreshold = exceptionThreshold;
    LOG.info("build topology");
  }

  public synchronized void start() {
    E2ETestUtils.retryFor(
        () -> E2ETestUtils.maybeCreateTopics(
            properties, partitions, List.of(inputTopic, outputTopic)),
        Duration.ofMinutes(5)
    );
    E2ETestUtils.retryFor(this::maybeCreateKeyspace, Duration.ofMinutes(5));
    // build topology after creating keyspace because we use keyspace retry
    // to wait for scylla to resolve
    kafkaStreams = buildTopology(properties);
    if (!stopped) {
      LOG.info("start kafka streams");
      kafkaStreams.start();
    }
  }

  public synchronized void stop() {
    if (!stopped) {
      if (kafkaStreams != null) {
        kafkaStreams.close();
      }
      stopped = true;
      this.notify();
    }
  }

  public synchronized void await() {
    while (!stopped) {
      try {
        this.wait();
      } catch (final InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private KafkaStreams buildTopology(final Map<String, Object> properties) {
    final StreamsBuilder builder = new StreamsBuilder();
    final KStream<Long, InputRecord> stream =
        builder.stream(inputTopic, Consumed.with(Serdes.Long(), Schema.inputRecordSerde()));
    final KStream<Long, OutputRecord> result = stream
        .processValues(AsyncFixedKeyProcessorSupplier.createAsyncProcessorSupplier(
            new E2ETestProcessorSupplier(name, exceptionThreshold)),
            Named.as("AsyncProcessor"),
            name
        );
    result.to(outputTopic, Produced.with(Serdes.Long(), Schema.outputRecordSerde()));
    final var builderProperties = new Properties();
    builderProperties.putAll(properties);
    builderProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, name);
    builderProperties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG,
        StreamsConfig.EXACTLY_ONCE_V2);
    builderProperties.put(ResponsiveConfig.ASYNC_THREAD_POOL_SIZE_CONFIG, 4);
    final var streams = new ResponsiveKafkaStreams(
        builder.build(builderProperties),
        builderProperties
    );
    streams.setUncaughtExceptionHandler(exception
        -> StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD);
    return streams;
  }

  private void maybeCreateKeyspace() {
    LOG.info("create keyspace responsive_test");
    try (final CqlSession session = cqlSession()) {
      final CreateKeyspace createKeyspace = SchemaBuilder.createKeyspace("responsive_test")
          .ifNotExists()
          .withSimpleStrategy(1);
      session.execute(createKeyspace.build());
    }
  }

  private CqlSession cqlSession() {
    final String scyllaName = properties.get(CASSANDRA_HOSTNAME_CONFIG).toString();
    final Integer port = Integer.parseInt(properties.get(CASSANDRA_PORT_CONFIG).toString());
    return CqlSession.builder()
        .addContactPoint(new InetSocketAddress(scyllaName, port))
        .withLocalDatacenter("datacenter1")
        .withConfigLoader(DriverConfigLoader.programmaticBuilder()
            .withLong(REQUEST_TIMEOUT, 100000)
            .build())
        .build();
  }

  static class E2ETestProcessor implements FixedKeyProcessor<Long, InputRecord, OutputRecord> {
    private final String storeName;
    private FixedKeyProcessorContext<Long, OutputRecord> context;
    private TimestampedKeyValueStore<Long, OutputRecord> store;
    private int partition;
    private final int exceptionThreshold;
    private final UrandomGenerator randomGenerator = new UrandomGenerator();

    E2ETestProcessor(final String storeName, final int exceptionThreshold) {
      this.storeName = Objects.requireNonNull(storeName);
      this.exceptionThreshold = exceptionThreshold;
    }

    @Override
    public void init(final FixedKeyProcessorContext<Long, OutputRecord> context) {
      this.context = context;
      this.store = context.getStateStore(storeName);
      this.partition = context.taskId().partition();
    }

    @Override
    public void process(final FixedKeyRecord<Long, InputRecord> record) {
      final var random = Math.abs(randomGenerator.nextLong() % 10000);
      if (random < exceptionThreshold) {
        throw new InjectedE2ETestException();
      }
      final ValueAndTimestamp<OutputRecord> old = store.get(record.key());
      final var in = record.value();
      AccumulatingChecksum checksum;
      if (old == null) {
        checksum = new AccumulatingChecksum();
      } else {
        checksum = new AccumulatingChecksum(old.value().digest());
      }
      final long offset = context.recordMetadata().get().offset();
      checksum = checksum.updateWith(in.count())
          .updateWith(offset)
          .updateWith(partition);
      final var out = new OutputRecord(
          in.value(),
          in.count(),
          context.recordMetadata().get().offset(),
          checksum.current()
      );
      store.put(record.key(), ValueAndTimestamp.make(out, record.timestamp()));
      context.forward(record.withValue(out));
    }

    @Override
    public void close() {
    }
  }

  static class E2ETestProcessorSupplier
      implements FixedKeyProcessorSupplier<Long, InputRecord, OutputRecord> {
    private final String storeName;
    private final int exceptionThreshold;

    E2ETestProcessorSupplier(final String storeName, final int exceptionThreshold) {
      this.storeName = Objects.requireNonNull(storeName);
      this.exceptionThreshold = exceptionThreshold;
    }

    @Override
    public FixedKeyProcessor<Long, InputRecord, OutputRecord> get() {
      return new E2ETestProcessor(storeName, exceptionThreshold);
    }

    @Override
    public Set<StoreBuilder<?>> stores() {
      return Set.of(
          ResponsiveStores.timestampedKeyValueStoreBuilder(
              ResponsiveStores.keyValueStore(ResponsiveKeyValueParams.keyValue(storeName)),
              Serdes.Long(),
              Schema.outputRecordSerde()
          )
      );
    }
  }

  private static class InjectedE2ETestException extends RuntimeException {
    private static final long serialVersionUID = 0L;

    public InjectedE2ETestException() {
      super("injected e2e test exception");
    }
  }
}
