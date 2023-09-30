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

import dev.responsive.kafka.api.stores.ResponsiveKeyValueParams;
import dev.responsive.kafka.api.stores.ResponsiveStores;
import dev.responsive.kafka.internal.stores.SchemaTypes;
import dev.responsive.kafka.internal.stores.SchemaTypes.KVSchema;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

public class ResponsiveTopologyTestDriverTest {

  private static ResponsiveKeyValueParams paramsForType(final KVSchema type) {
    return type == KVSchema.KEY_VALUE
        ? ResponsiveKeyValueParams.keyValue("people")
        : ResponsiveKeyValueParams.fact("people");
  }

  @ParameterizedTest
  @EnumSource(SchemaTypes.KVSchema.class)
  public void shouldRunAllKVStoreTypesWithoutResponsiveConnection(final KVSchema type) {
    // Given:
    final TopologyTestDriver driver = setupDriver(paramsForType(type));

    final TestInputTopic<String, String> bids = driver.createInputTopic(
        "bids", new StringSerializer(), new StringSerializer());
    final TestInputTopic<String, String> people = driver.createInputTopic(
        "people", new StringSerializer(), new StringSerializer());
    final TestOutputTopic<String, String> output = driver.createOutputTopic(
        "output", new StringDeserializer(), new StringDeserializer());

    // When:
    people.pipeInput("1", "1,alice,CA");
    people.pipeInput("2", "2,bob,OR");
    people.pipeInput("3", "3,carol,CA");

    bids.pipeInput("a", "a,100,1");
    bids.pipeInput("b", "b,101,2");
    bids.pipeInput("c", "c,102,1");
    bids.pipeInput("d", "d,103,3");

    // Then:
    final List<String> outputs = output.readValuesToList();
    MatcherAssert.assertThat(outputs, Matchers.contains(
        "a,100,1,1,alice,CA",
        "c,102,1,1,alice,CA",
        "d,103,3,3,carol,CA"
    ));
    driver.close();
  }

  @ParameterizedTest
  @EnumSource(SchemaTypes.KVSchema.class)
  public void shouldRunAllKVStoreTypesWithTTLWithoutResponsiveConnection(final KVSchema type) {
    // Given:
    final Duration ttl = Duration.ofMillis(15);
    final TopologyTestDriver driver = setupDriver(
        paramsForType(type).withTimeToLive(ttl)
    );

    final TestInputTopic<String, String> bids = driver.createInputTopic(
        "bids", new StringSerializer(), new StringSerializer());
    final TestInputTopic<String, String> people = driver.createInputTopic(
        "people", new StringSerializer(), new StringSerializer());
    final TestOutputTopic<String, String> output = driver.createOutputTopic(
        "output", new StringDeserializer(), new StringDeserializer());

    // When:
    people.pipeInput("1", "1,alice,CA"); // time = 0
    driver.advanceWallClockTime(Duration.ofMillis(5));
    people.pipeInput("2", "2,bob,OR");   // time = 5
    driver.advanceWallClockTime(Duration.ofMillis(5));
    people.pipeInput("3", "3,carol,CA"); // time = 10
    
    bids.pipeInput("a", "a,100,1");      // time = 10
    bids.pipeInput("b", "b,101,2");      // time = 10

    driver.advanceWallClockTime(Duration.ofMillis(10));
    bids.pipeInput("c", "c,102,1");      // time = 20 -- no result because alice has expired x.x
    bids.pipeInput("d", "d,103,3");      // time = 20

    people.pipeInput("1", "1,alex,CA");  // time = 20
    bids.pipeInput("e", "e,104,1");      // time = 20 -- new result as alex has replaced alice

    driver.advanceWallClockTime(Duration.ofMillis(10));
    bids.pipeInput("f", "f,105,3");      // time = 30 -- no result because carol has expired x.x

    // Then:
    final List<String> outputs = output.readValuesToList();
    MatcherAssert.assertThat(outputs, Matchers.contains(
        "a,100,1,1,alice,CA",
        "d,103,3,3,carol,CA",
        "e,104,1,1,alex,CA"
        ));
    driver.close();
  }

  private ResponsiveTopologyTestDriver setupDriver(final ResponsiveKeyValueParams storeParams) {
    final Properties props = new Properties();
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);

    final Topology topology = topology(storeParams);
    return new ResponsiveTopologyTestDriver(topology, props, Instant.EPOCH);
  }

  private Topology topology(final ResponsiveKeyValueParams storeParams) {
    final StreamsBuilder builder = new StreamsBuilder();

    // schema for bids is key: <bid_id> value: <bid_id, amount, person_id>
    final KStream<String, String> bids = builder.stream("bids");
    // schema for people is key: <person_id> value: <person_id, name, state>
    final KTable<String, String> people = builder.table(
        "people",
        ResponsiveStores.materialized(storeParams)
    );

    bids
        // person_id is 3rd column
        .selectKey((k, v) -> v.split(",")[2])
        // schema is now <bid_id, amount, person_id, name, state>
        .join(people, (bid, person) -> bid + "," + person)
        // state is the 6th column
        .filter((k, v) -> v.split(",")[5].equals("CA"))
        .to("output");

    return builder.build();
  }

}
