package org.apache.kafka.streams;

import static org.apache.kafka.streams.processor.internals.ProcessorStateManager.storeChangelogTopic;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

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
}
