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

package dev.responsive.examples.e2etest;

import dev.responsive.examples.common.E2ETestUtils;
import dev.responsive.examples.common.InjectedE2ETestException;
import dev.responsive.examples.common.UncaughtStreamsAntithesisHandler;
import dev.responsive.examples.e2etest.E2ESchema.InputRecord;
import dev.responsive.examples.e2etest.E2ESchema.OutputRecord;
import dev.responsive.kafka.api.ResponsiveKafkaStreams;
import dev.responsive.kafka.api.async.AsyncFixedKeyProcessorSupplier;
import dev.responsive.kafka.api.config.ResponsiveConfig;
import dev.responsive.kafka.api.stores.ResponsiveKeyValueParams;
import dev.responsive.kafka.api.stores.ResponsiveStores;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
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
    E2ETestUtils.maybeCreateTopics(properties, partitions, List.of(inputTopic, outputTopic));
    E2ETestUtils.maybeCreateKeyspace(properties);
    // build topology after creating keyspace because we use keyspace retry
    // to wait for scylla to resolve
    properties.put(ResponsiveConfig.PLATFORM_API_KEY_CONFIG, "test-api-key");
    E2ETestUtils.retryFor(
        () -> kafkaStreams = buildTopology(properties),
        Duration.ofMinutes(5)
    );
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
        builder.stream(inputTopic, Consumed.with(Serdes.Long(), E2ESchema.inputRecordSerde()));
    final UrandomGenerator randomGenerator = new UrandomGenerator();
    int exceptionThreshold = this.exceptionThreshold;
    if (Math.abs(randomGenerator.nextLong()) % 2 == 0) {
      exceptionThreshold = 0;
    }
    final KStream<Long, OutputRecord> result = stream
        .processValues(AsyncFixedKeyProcessorSupplier.createAsyncProcessorSupplier(
            new E2ETestProcessorSupplier(name, exceptionThreshold)),
            Named.as("AsyncProcessor"),
            name
        );
    result.to(outputTopic, Produced.with(Serdes.Long(), E2ESchema.outputRecordSerde()));
    final var builderProperties = new Properties();
    builderProperties.putAll(properties);
    builderProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, name);
    builderProperties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2);
    builderProperties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);
    builderProperties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 60000);
    builderProperties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10);
    builderProperties.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 90000);
    builderProperties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG,
        StreamsConfig.EXACTLY_ONCE_V2);
    builderProperties.put(ResponsiveConfig.ASYNC_THREAD_POOL_SIZE_CONFIG, 2);
    final var streams = new ResponsiveKafkaStreams(
        builder.build(builderProperties),
        builderProperties
    );
    streams.setUncaughtExceptionHandler(new UncaughtStreamsAntithesisHandler());
    return streams;
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
              E2ESchema.outputRecordSerde()
          )
      );
    }
  }

}
