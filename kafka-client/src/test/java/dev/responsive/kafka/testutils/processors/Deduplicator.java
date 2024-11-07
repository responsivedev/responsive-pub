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

package dev.responsive.kafka.testutils.processors;

import static dev.responsive.kafka.testutils.processors.GenericProcessorSuppliers.getFixedKeySupplier;

import dev.responsive.kafka.api.stores.ResponsiveKeyValueParams;
import dev.responsive.kafka.api.stores.ResponsiveStores;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

public class Deduplicator {

  public static Topology deduplicatorApp(
      final String inputTopicName,
      final String outputTopicName,
      final ResponsiveKeyValueParams params
  ) {
    final StreamsBuilder builder = new StreamsBuilder();
    final KStream<String, String> input = builder.stream(inputTopicName);

    final var storeBuilder = ResponsiveStores.timestampedKeyValueStoreBuilder(
        ResponsiveStores.keyValueStore(params), Serdes.String(), Serdes.String()
    );
    final String storeName = params.name().kafkaName();
    input
        .processValues(getFixedKeySupplier(DeduplicatorProcessor::new, storeBuilder), storeName)
        .to(outputTopicName);

    return builder.build();
  }

  private static class DeduplicatorProcessor implements FixedKeyProcessor<String, String, String> {

    private final String storeName;

    private FixedKeyProcessorContext<String, String> context;
    private TimestampedKeyValueStore<String, String> ttlStore;

    public DeduplicatorProcessor(final String storeName) {
      this.storeName = storeName;
    }

    @Override
    public void init(final FixedKeyProcessorContext<String, String> context) {
      this.context = context;
      this.ttlStore = context.getStateStore(storeName);
    }

    @Override
    public void process(final FixedKeyRecord<String, String> record) {
      final ValueAndTimestamp<String> previous = ttlStore.putIfAbsent(
          record.key(),
          ValueAndTimestamp.make(record.value(), record.timestamp())
      );

      if (previous == null) {
        context.forward(record);
      }

    }
  }
}
