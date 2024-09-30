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

package dev.responsive.examples.regression;

import static dev.responsive.examples.regression.RegConstants.NUM_PARTITIONS;
import static dev.responsive.examples.regression.RegConstants.resultsTopic;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ISOLATION_LEVEL_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import dev.responsive.examples.common.EventSignals;
import dev.responsive.examples.common.JsonDeserializer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResultsComparatorService<T extends Comparable<T>>
    extends AbstractExecutionThreadService {

  private static final Logger LOG = LoggerFactory.getLogger(ResultsComparatorService.class);
  private static final int MAX_BUFFER = 100;

  private final Map<String, Object> properties;
  private KafkaConsumer<String, T> consumer;

  public ResultsComparatorService(
      final Map<String, Object> properties,
      final Class<? extends JsonDeserializer<T>> deserializerClass
  ) {
    this.properties = new HashMap<>(properties);
    this.properties.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    this.properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, deserializerClass);
    this.properties.put(ISOLATION_LEVEL_CONFIG, "read_committed");
  }

  @Override
  protected void startUp() throws Exception {
    // note that since this runs after we've started the
    // OrderAndCustomerDriver, we can assume that the driver has
    // already created the topics
    consumer = new KafkaConsumer<>(properties);
    consumer.assign(topicPartitions());
    consumer.seekToBeginning(topicPartitions());
  }

  @Override
  protected void shutDown() throws Exception {
    consumer.close();
  }

  @Override
  protected void run() throws Exception {
    final int[] responsiveConsumed = new int[NUM_PARTITIONS];
    final int[] baselineConsumed = new int[NUM_PARTITIONS];

    final var baselineBuffered = ArrayListMultimap.<Integer, Record<T>>create();
    final var responsiveBuffered = ArrayListMultimap.<Integer, Record<T>>create();

    boolean loggedStartSignal = false;
    int matches = 0;
    while (isRunning()) {
      // avoid buffering too many records if the difference between
      // consumed is significant
      pauseResume(responsiveConsumed, baselineConsumed);

      final var records = consumer.poll(Duration.ofSeconds(10));

      if (!loggedStartSignal && !records.isEmpty()) {
        EventSignals.logNumConsumedOutputRecords(records.count());
        loggedStartSignal = true;
      }

      records.records(resultsTopic(true)).forEach(r -> {
        responsiveConsumed[r.partition()]++;
        responsiveBuffered.put(r.partition(), new Record<>(r));
      });
      records.records(resultsTopic(false)).forEach(r -> {
        baselineConsumed[r.partition()]++;
        baselineBuffered.put(r.partition(), new Record<>(r));
      });

      for (int p = 0; p < NUM_PARTITIONS; p++) {
        var responsive = responsiveBuffered.get(p).iterator();
        var baseline = baselineBuffered.get(p).iterator();

        while (responsive.hasNext() && baseline.hasNext()) {
          final var r = responsive.next();
          final var b = baseline.next();

          if (r.compareTo(b) == 0) {
            responsive.remove();
            baseline.remove();
            matches++;
          } else if (responsive.hasNext() && baseline.hasNext()) {
            LOG.error(
                "ANTITHESIS NEVER: Expected to consume records in order, but they were not"
                    + "in order. Most recent record from responsive is {} and most recent up"
                    + "record from baseline is {}",
                r.record,
                b.record
            );
          } else {
            // one of the streams is behind so we'll wait for the next
            // poll to see if any records come up here
            break;
          }
        }
      }

      LOG.info(
          "Matched {} records. Current buffers are: responsive={}, baseline={}",
          matches,
          responsiveBuffered.size(),
          baselineBuffered.size()
      );
    }
  }

  private void pauseResume(final int[] consumedResponsive, final int[] consumedBaseline) {
    final List<TopicPartition> toPause = new ArrayList<>();
    final List<TopicPartition> toResume = new ArrayList<>();

    for (int p = 0; p < NUM_PARTITIONS; p++) {
      final int compare = consumedResponsive[p] - consumedBaseline[p];
      if (compare > MAX_BUFFER) {
        LOG.warn(
            "partition {} on responsive is ahead of baseline by {} records... Pausing responsive.",
            p,
            compare
        );
        toPause.add(new TopicPartition(resultsTopic(true), p));
        toResume.add(new TopicPartition(resultsTopic(false), p));
      } else if (compare < -MAX_BUFFER) {
        LOG.warn(
            "partition {} on responsive is behind of baseline by {} records... Pausing baseline.",
            p,
            -compare
        );
        toPause.add(new TopicPartition(resultsTopic(false), p));
        toResume.add(new TopicPartition(resultsTopic(true), p));
      } else {
        toResume.add(new TopicPartition(resultsTopic(true), p));
        toResume.add(new TopicPartition(resultsTopic(false), p));
      }
    }

    consumer.pause(toPause);
    consumer.resume(toResume);
  }

  private static List<TopicPartition> topicPartitions() {
    return IntStream
        .range(0, NUM_PARTITIONS)
        .boxed()
        .flatMap(p -> Stream.of(
            new TopicPartition(resultsTopic(true), p),
            new TopicPartition(resultsTopic(false), p)
        )).toList();
  }

  private record Record<T extends Comparable<T>>(ConsumerRecord<String, T> record)
      implements Comparable<Record<T>> {

    @Override
    public int compareTo(final Record<T> o) {
      return record.value().compareTo(o.record().value());
    }
  }
}
