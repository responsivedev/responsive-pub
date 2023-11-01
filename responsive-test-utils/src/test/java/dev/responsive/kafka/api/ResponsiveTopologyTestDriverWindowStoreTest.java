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

package dev.responsive.kafka.api;

import static java.lang.Long.parseLong;
import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.equalTo;

import dev.responsive.kafka.api.stores.ResponsiveStores;
import dev.responsive.kafka.api.stores.ResponsiveWindowParams;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Test;

public class ResponsiveTopologyTestDriverWindowStoreTest {

  private static final Duration windowSize = Duration.ofMillis(10);
  private static final Duration gracePeriod = Duration.ofMillis(5);

  @Test
  public void shouldRunWindowStoreWithoutResponsiveConnection() {
    // Given:
    final ResponsiveWindowParams params =
        ResponsiveWindowParams.window("ttd-window-store", windowSize, gracePeriod);
    final TopologyTestDriver driver = setupDriver(params);

    final TestInputTopic<String, Long> bids = driver.createInputTopic(
        "bids", new StringSerializer(), new LongSerializer());
    final TestOutputTopic<String, Long> output = driver.createOutputTopic(
        "output", new StringDeserializer(), new LongDeserializer());

    // When:
    bids.pipeInput("luna:1", 1L); // streamTime = 1     update <[0-9], 1>
    bids.pipeInput("luna:6", 2L); // streamTime = 6     update <[0-9], 3>
    bids.pipeInput("luna:10", 3L); // streamTime = 10   update <[10-19], 3>
    bids.pipeInput("luna:5", 4L); // streamTime = 10    update <[0-9], 7>
    bids.pipeInput("luna:18", 5L); // streamTime = 18   update <[10-19], 8>
    bids.pipeInput("luna:2", 6L); // streamTime = 18    no update (outside grace)
    bids.pipeInput("luna:23", 7L); // time = 23         update <[20-29], 7>

    // Then:
    final List<Long> outputs = output.readValuesToList();
    MatcherAssert.assertThat(outputs, equalTo(
        asList(1L, 3L, 3L, 7L, 8L, 7L)
    ));
    driver.close();
  }

  private ResponsiveTopologyTestDriver setupDriver(final ResponsiveWindowParams storeParams) {
    final Properties props = new Properties();
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.LongSerde.class);

    final Topology topology = topology(storeParams);
    return new ResponsiveTopologyTestDriver(topology, props, Instant.EPOCH);
  }

  private Topology topology(final ResponsiveWindowParams params) {
    final StreamsBuilder builder = new StreamsBuilder();

    // schema for bids is <person_id:timestamp, bid_amount>
    final KStream<String, Long> bids = builder.stream(
        "bids",
        Consumed.with((record, t) -> parseLong(extractFieldFromKey((String) record.key(), 1))));

    bids
        .selectKey((k, v) -> extractFieldFromKey(k, 0))
        // schema here --> <person_id, bid_amount>
        .groupByKey()
        .windowedBy(TimeWindows.ofSizeAndGrace(windowSize, gracePeriod))
        // schema here --> <window<person_id>, total_bid>
        .reduce(Long::sum, ResponsiveStores.windowMaterialized(params))
        .toStream()
        // schema here --> <person_id: [window], total_bid>
        .selectKey((w, v) -> w.key() + ": " + w.window().toString())
        .to("output");

    return builder.build();
  }

  final String extractFieldFromKey(final String key, final int index) {
    return key.split(":")[index];
  }
}
