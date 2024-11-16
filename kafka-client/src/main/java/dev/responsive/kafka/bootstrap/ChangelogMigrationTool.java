/*
 * Copyright 2024 Responsive Computing, Inc.
 *
 * This source code is licensed under the Responsive Software License Agreement v1.0 available at
 *
 * https://www.responsive.dev/legal/responsive-bsl-10
 *
 * This software requires a valid Commercial License Key for production use. Trial and commercial
 * licenses can be obtained at https://www.responsive.dev/sdk/get-started.
 */

package dev.responsive.kafka.bootstrap;

import static dev.responsive.kafka.api.config.ResponsiveConfig.RESPONSIVE_MODE;
import static dev.responsive.kafka.api.config.ResponsiveConfig.STORE_FLUSH_RECORDS_TRIGGER_CONFIG;
import static dev.responsive.kafka.api.config.ResponsiveConfig.WRITE_CONSISTENCY_LEVEL_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.REUSE_KTABLE_SOURCE_TOPICS;
import static org.apache.kafka.streams.StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import dev.responsive.kafka.api.ResponsiveKafkaStreams;
import dev.responsive.kafka.api.config.ResponsiveMode;
import dev.responsive.kafka.api.stores.ResponsiveKeyValueParams;
import dev.responsive.kafka.api.stores.ResponsiveStores;
import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

public class ChangelogMigrationTool {

  private static final org.slf4j.Logger LOG =
      org.slf4j.LoggerFactory.getLogger(ChangelogMigrationTool.class);

  private final Consumer<Record<byte[], byte[]>> processor;
  private final String changelogTopic;
  private final Properties properties;
  private final ResponsiveKeyValueParams params;

  @SuppressWarnings("unused") // reason: public API
  public ChangelogMigrationTool(
      final Properties properties,
      final ResponsiveKeyValueParams params,
      final String changelogTopic
  ) {
    this(properties, params, changelogTopic, r -> {});
  }

  // Visible for testing
  ChangelogMigrationTool(
      final Properties properties,
      final ResponsiveKeyValueParams params,
      final String changelogTopic,
      final Consumer<Record<byte[], byte[]>> processor
  ) {
    this.processor = processor;

    properties.put(TOPOLOGY_OPTIMIZATION_CONFIG, REUSE_KTABLE_SOURCE_TOPICS);
    properties.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
    properties.put(RESPONSIVE_MODE, ResponsiveMode.MIGRATE.name());
    properties.put(WRITE_CONSISTENCY_LEVEL_CONFIG, ConsistencyLevel.ALL.name());

    // it is possible to push this number higher, but this is a relatively safe
    // number and still gets decent performance -- use putIfAbsent in case the
    // customer wants to override this to push more performance at the risk of
    // stability
    properties.putIfAbsent(STORE_FLUSH_RECORDS_TRIGGER_CONFIG, 10_000);

    this.properties = properties;
    this.params = params;
    this.changelogTopic = changelogTopic;
  }

  public ResponsiveKafkaStreams buildStreams() {
    final StreamsBuilder builder = new StreamsBuilder();

    final KTable<byte[], byte[]> table =
        builder.table(
            changelogTopic,
            Consumed.with(Serdes.ByteArray(), Serdes.ByteArray()),
            Materialized
                .<byte[], byte[]>as(ResponsiveStores.keyValueStore(params))
                .withValueSerde(Serdes.ByteArray())
                .withKeySerde(Serdes.ByteArray())
    );

    table
        .toStream()
        .process(() -> (new Processor<byte[], byte[], byte[], byte[]>() {

          private final AtomicLong lastLogged = new AtomicLong();
          private ProcessorContext<byte[], byte[]> context;

          @Override
          public void init(final ProcessorContext<byte[], byte[]> context) {
            this.context = context;
          }

          @Override
          public void process(final Record<byte[], byte[]> record) {
            processor.accept(record);
            if (context.currentSystemTimeMs() - lastLogged.get() > TimeUnit.SECONDS.toMillis(30)) {
              lastLogged.set(context.currentSystemTimeMs());
              final long ms = context.currentStreamTimeMs();
              LOG.info("Migration has restored task {} up until stream time {}",
                  context.taskId(),
                  Instant.ofEpochMilli(ms));
            }
          }
        }));

    return new ResponsiveKafkaStreams(builder.build(properties), properties);
  }
}
