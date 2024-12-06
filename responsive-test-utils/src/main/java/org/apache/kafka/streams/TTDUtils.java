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

package org.apache.kafka.streams;

import static org.apache.kafka.streams.processor.internals.ProcessorStateManager.storeChangelogTopic;

import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.test.TestRecord;

/**
 * A utility class that lives in the o.a.k.streams package so we can access
 * internal topology metadata such as topics
 */
public final class TTDUtils {

  /**
   * @param appId the application id
   * @param stores the list of state store names for which to derive changelog topic names
   * @return the set of expected changelog topics computed for the provided state store names
   */
  public static Set<String> deriveChangelogTopic(final String appId, final List<String> stores) {
    return stores
        .stream()
        .map(s -> storeChangelogTopic(appId, s, null))
        .collect(Collectors.toSet());
  }

  /**
   * @param topology a compiled topology (must have already been initialized by the TTD/app)
   * @return the set of actual changelog topics belonging to all state stores in this topology
   */
  public static Set<String> extractChangelogTopics(final Topology topology) {
    return topology.internalTopologyBuilder
        .subtopologyToTopicsInfo().values()
        .stream()
        .flatMap(t -> t.stateChangelogTopics.keySet().stream())
        .collect(Collectors.toSet());
  }

  public static class TopologyTestDriverAccessor extends TopologyTestDriver {

    private final Properties props;

    public TopologyTestDriverAccessor(
        final Topology topology,
        final Properties config,
        final Instant initialWallClockTime
    ) {
      super(topology, config, initialWallClockTime);
      this.props = config;
    }

    public Properties props() {
      return props;
    }

    @Override
    protected <K, V> void pipeRecord(final String topic,
                                     final TestRecord<K, V> record,
                                     final Serializer<K> keySerializer,
                                     final Serializer<V> valueSerializer,
                                     final Instant time) {
      super.pipeRecord(topic, record, keySerializer, valueSerializer, time);
    }

  }
}
