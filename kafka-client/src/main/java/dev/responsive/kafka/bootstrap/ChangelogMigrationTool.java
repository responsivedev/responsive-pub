/*
 * Copyright 2023 Responsive Computing, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dev.responsive.kafka.bootstrap;

import static dev.responsive.kafka.bootstrap.ChangelogMigrationConfig.CHANGELOG_TOPIC_CONFIG;
import static dev.responsive.kafka.bootstrap.ChangelogMigrationConfig.TABLE_NAME_CONFIG;

import dev.responsive.kafka.api.ResponsiveKafkaStreams;
import dev.responsive.kafka.api.config.ResponsiveConfig;
import dev.responsive.kafka.api.config.ResponsiveMode;
import dev.responsive.kafka.api.stores.ResponsiveStores;
import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

public class ChangelogMigrationTool {

  private static final org.slf4j.Logger LOG =
      org.slf4j.LoggerFactory.getLogger(ChangelogMigrationTool.class);

  private final ResponsiveKafkaStreams streams;

  public ChangelogMigrationTool(final Properties properties) {
    properties.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    properties.put(ResponsiveConfig.RESPONSIVE_MODE, ResponsiveMode.MIGRATE.name());

    // it is possible to push this number higher, but this is a relatively safe
    // number and still gets decent performance -- use putIfAbsent in case the
    // customer wants to override this to push more performance at the risk of
    // stability
    properties.putIfAbsent(ResponsiveConfig.STORE_FLUSH_RECORDS_TRIGGER_CONFIG, 10_000);

    final ChangelogMigrationConfig config = new ChangelogMigrationConfig(properties);
    this.streams = buildStreams(config, properties);
  }

  public ResponsiveKafkaStreams getStreams() {
    return streams;
  }

  private ResponsiveKafkaStreams buildStreams(
      final ChangelogMigrationConfig config,
      final Properties properties
  ) {
    final StreamsBuilder builder = new StreamsBuilder();
    final String source = config.getString(CHANGELOG_TOPIC_CONFIG);
    final String tableName = config.getString(TABLE_NAME_CONFIG);
    final AtomicLong processed = new AtomicLong();

    final KTable<byte[], byte[]> table =
        builder.table(
            source,
            Materialized.as(ResponsiveStores.factStore(tableName))
    );

    table
        .toStream()
        .process(() -> (new Processor<byte[], byte[], byte[], byte[]>() {
          private ProcessorContext<byte[], byte[]> context;

          @Override
          public void init(final ProcessorContext<byte[], byte[]> context) {
            this.context = context;
          }

          @Override
          public void process(final Record<byte[], byte[]> record) {
            if (processed.incrementAndGet() % 10_000 == 0) {
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
