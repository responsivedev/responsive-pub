package dev.responsive.kafka.internal.clients;

import static org.apache.kafka.streams.TTDUtils.deriveChangelogTopic;
import static org.apache.kafka.streams.TTDUtils.extractChangelogTopics;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.MockAdminClient;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.TopologyDescription.Processor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TTDMockAdmin extends MockAdminClient {
  private static final Logger LOG = LoggerFactory.getLogger(TTDMockAdmin.class);
  private static final Node BROKER = new Node(0, "dummyHost-1", 1234);

  private final Properties props;
  private final Topology topology;
  private final Set<String> createdTopics = new HashSet<>();

  public TTDMockAdmin(final Properties props, final Topology topology) {
    super(Collections.singletonList(BROKER), BROKER);
    this.props = props;
    this.topology = topology;

    final List<String> stateStoreNames = new LinkedList<>();
    final var processors = topology.describe()
        .subtopologies()
        .stream().flatMap(s -> s.nodes().stream())
        .collect(Collectors.toList());

    for (final TopologyDescription.Node node : processors) {
      if (node instanceof Processor) {
        stateStoreNames.addAll(((Processor) node).stores());
      }
    }

    final String applicationId = props.getProperty(StreamsConfig.APPLICATION_ID_CONFIG);

    for (final String topic : deriveChangelogTopic(applicationId, stateStoreNames)) {
      addTopic(
          false,
          topic,
          Collections.singletonList(new TopicPartitionInfo(
              0, BROKER, Collections.emptyList(), Collections.emptyList())
          ),
          Collections.singletonMap(
              TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT)
      );
      createdTopics.add(topic);
    }
    LOG.debug("Initialized TTD mock admin with changelog topics = [{}]", createdTopics);
  }

  public Properties props() {
    return props;
  }

  public void verifyChangelogTopicCreation() {
    final Set<String> missingTopics = new HashSet<>(extractChangelogTopics(topology));
    missingTopics.retainAll(createdTopics);
    if (!missingTopics.isEmpty()) {
      LOG.warn("Not all changelog topics were pre-initialized, missing topics=[{}]", missingTopics);
    }
  }
}
