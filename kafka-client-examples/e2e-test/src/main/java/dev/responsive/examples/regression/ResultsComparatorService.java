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

package dev.responsive.examples.regression;

import static dev.responsive.examples.common.E2ETestUtils.buildAssertionContext;
import static dev.responsive.examples.regression.RegConstants.NUM_PARTITIONS;
import static dev.responsive.examples.regression.RegConstants.resultsTopic;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ISOLATION_LEVEL_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

import com.antithesis.sdk.Assert;
import com.antithesis.sdk.Lifecycle;
import com.fasterxml.jackson.databind.node.ObjectNode;
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

    boolean setupCompleteSignalFired = false;
    int matches = 0;
    while (isRunning()) {
      // avoid buffering too many records if the difference between
      // consumed is significant
      pauseResume(responsiveConsumed, baselineConsumed);

      final var records = consumer.poll(Duration.ofSeconds(10));

      // Signal that the setup is complete we start seeing output records
      // from the Responsive app and are ready to start injecting failures
      if (!setupCompleteSignalFired
          && records.records(resultsTopic(true)).iterator().hasNext()) {
        LOG.info("Received at least one output record, setup is complete");
        Lifecycle.setupComplete(null);
        setupCompleteSignalFired = true;

        EventSignals.logNumConsumedOutputRecords(records.count());
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

        Assert.sometimes(responsive.hasNext(), "Saw Responsive app output", null);
        Assert.sometimes(baseline.hasNext(), "Saw baseline app output", null);

        while (responsive.hasNext() && baseline.hasNext()) {
          final var r = responsive.next();
          final var b = baseline.next();

          if (r.compareTo(b) == 0) {
            responsive.remove();
            baseline.remove();
            matches++;
          } else if (responsive.hasNext() && baseline.hasNext()) {
            final String errorMessage = String.format(
                "Expected to see identical output records in identical order, but the next set "
                    + "of records did not match up. Most recent record from responsive is %s "
                    + "and most recent record from baseline is %s",
                r.record,
                b.record);

            final ObjectNode errorDetails = buildAssertionContext(errorMessage);
            errorDetails.put("responsiveKey", r.record.key());
            errorDetails.put("baselineKey", b.record.key());
            errorDetails.put("responsiveValue", r.record.value().toString());
            errorDetails.put("baselineValue", b.record.value().toString());
            errorDetails.put("responsiveRecord", r.record.toString());
            errorDetails.put("baselineRecord", b.record.toString());

            Assert.unreachable("Mismatch between next record of Responsive & baseline",
                               errorDetails);

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
