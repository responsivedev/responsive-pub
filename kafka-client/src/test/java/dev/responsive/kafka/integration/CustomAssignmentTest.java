/*
 *  Copyright 2024 Responsive Computing, Inc.
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

package dev.responsive.kafka.integration;

import static dev.responsive.kafka.testutils.IntegrationTestUtils.createTopicsAndWait;
import static dev.responsive.kafka.testutils.IntegrationTestUtils.pipeInput;
import static dev.responsive.kafka.testutils.IntegrationTestUtils.startAppAndAwaitRunning;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.TRANSACTION_TIMEOUT_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_SERVER_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.COMMIT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.EXACTLY_ONCE_V2;
import static org.apache.kafka.streams.StreamsConfig.NUM_STREAM_THREADS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.PROCESSING_GUARANTEE_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.consumerPrefix;
import static org.apache.kafka.streams.StreamsConfig.producerPrefix;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import dev.responsive.kafka.api.ResponsiveKafkaStreams;
import dev.responsive.kafka.api.stores.ResponsiveKeyValueParams;
import dev.responsive.kafka.api.stores.ResponsiveStores;
import dev.responsive.kafka.internal.stores.SchemaTypes;
import dev.responsive.kafka.testutils.ResponsiveConfigParam;
import dev.responsive.kafka.testutils.ResponsiveExtension;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.assignment.ApplicationState;
import org.apache.kafka.streams.processor.assignment.KafkaStreamsAssignment;
import org.apache.kafka.streams.processor.assignment.KafkaStreamsState;
import org.apache.kafka.streams.processor.assignment.ProcessId;
import org.apache.kafka.streams.processor.assignment.TaskAssignor;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ExtendWith(ResponsiveExtension.class)
public class CustomAssignmentTest {
    private static final Logger LOG = LoggerFactory.getLogger(CustomAssignmentTest.class);

    private static final int MAX_POLL_MS = 5000;
    private static final String INPUT_TOPIC = "input";
    private static final String OUTPUT_TOPIC = "output";

    private final Map<String, Object> responsiveProps = new HashMap<>();

    private String name;
    private Admin admin;
    private ScheduledExecutorService executor;

    @BeforeEach
    public void before(
        final TestInfo info,
        final Admin admin,
        @ResponsiveConfigParam final Map<String, Object> responsiveProps
    ) {
        // add displayName to name to account for parameterized tests
        name = info.getTestMethod().orElseThrow().getName() + "-" + new Random().nextInt();
        executor = new ScheduledThreadPoolExecutor(2);

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
    public void shouldUseCustomAssignorInRebalance() throws Exception {
        // Given:
        final Map<String, Object> properties = getMutableProperties();
        final KafkaProducer<Long, Long> producer = new KafkaProducer<>(properties);
        final SharedState state = new SharedState();

        // When:
        try (
            final ResponsiveKafkaStreams streamsA = buildStreams(properties, "a", state);
        ) {
            startAppAndAwaitRunning(Duration.ofSeconds(10), streamsA);
            pipeInput(inputTopic(), 2, producer, System::currentTimeMillis, 0, 10, 0, 1, 2, 3);

            Thread.sleep(5_000);
            assertThat(state.numRecords.get(), equalTo(0));
        }
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
        properties.put(NUM_STREAM_THREADS_CONFIG, 1);

        // this ensures we can control the commits by explicitly requesting a commit
        properties.put(COMMIT_INTERVAL_MS_CONFIG, 20_000);
        properties.put(producerPrefix(TRANSACTION_TIMEOUT_CONFIG), 20_000);

        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
        properties.put(consumerPrefix(ConsumerConfig.METADATA_MAX_AGE_CONFIG), "1000");
        properties.put(consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest");
        properties.put(consumerPrefix(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG), MAX_POLL_MS);
        properties.put(consumerPrefix(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG), MAX_POLL_MS);
        properties.put(consumerPrefix(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG), MAX_POLL_MS - 1);

        properties.put(StreamsConfig.TASK_ASSIGNOR_CLASS_CONFIG, CompleteStallAssignor.class.getName());

        return properties;
    }

    private StoreBuilder<KeyValueStore<Long, Long>> storeSupplier(SchemaTypes.KVSchema type) {
        return ResponsiveStores.keyValueStoreBuilder(
            ResponsiveStores.keyValueStore(
                type == SchemaTypes.KVSchema.FACT
                    ? ResponsiveKeyValueParams.fact(name)
                    : ResponsiveKeyValueParams.keyValue(name)
            ),
            Serdes.Long(),
            Serdes.Long()
        ).withLoggingEnabled(
            Map.of(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE));
    }

    private ResponsiveKafkaStreams buildStreams(
        final Map<String, Object> originals,
        final String instance,
        final SharedState state
    ) {
        final Map<String, Object> properties = new HashMap<>(originals);
        properties.put(APPLICATION_SERVER_CONFIG, instance + ":1024");

        final StreamsBuilder builder = new StreamsBuilder();
        builder.addStateStore(storeSupplier(SchemaTypes.KVSchema.KEY_VALUE));

        final KStream<Long, Long> input = builder.stream(inputTopic());
        input
            .process(() -> new TestProcessor(instance, state, name), name)
            .to(outputTopic());

        return new ResponsiveKafkaStreams(builder.build(), properties);
    }

    public static class CompleteStallAssignor implements TaskAssignor {
        @Override
        public TaskAssignment assign(final ApplicationState applicationState) {
            final Map<ProcessId, KafkaStreamsAssignment> assignments = new HashMap<>();
            final Collection<KafkaStreamsState> states = applicationState.kafkaStreamsStates(false).values();
            for (final KafkaStreamsState state : states) {
                LOG.info("Client state: {} | {}", state.processId(), state.numProcessingThreads());
                assignments.put(state.processId(), KafkaStreamsAssignment.of(state.processId(), new HashSet<>()));
            }

            return new TaskAssignment(assignments.values());
        }
    }

    private static class SharedState {
        private final AtomicInteger numRecords = new AtomicInteger(0);
    }

    private static class TestProcessor implements Processor<Long, Long, Long, Long> {

        private final String instance;
        private final SharedState state;
        private final String name;
        private ProcessorContext<Long, Long> context;
        private KeyValueStore<Long, Long> store;

        public TestProcessor(final String instance, final SharedState state, String name) {
            this.instance = instance;
            this.state = state;
            this.name = name;
        }

        @Override
        public void init(final ProcessorContext<Long, Long> context) {
            this.context = context;
            this.store = context.getStateStore(name);
        }

        @Override
        public void process(final Record<Long, Long> record) {
            state.numRecords.incrementAndGet();
            final long sum = updateSum(record.key(), record.value());
            context.forward(new Record<>(record.key(), sum, System.currentTimeMillis()));
        }

        private long updateSum(final long key, final long value) {
            Long sum = store.get(key);
            sum = (sum == null) ? value : sum + value;
            store.put(key, sum);
            return sum;
        }
    }
}
