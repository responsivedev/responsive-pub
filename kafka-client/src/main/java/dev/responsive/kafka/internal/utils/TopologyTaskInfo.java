package dev.responsive.kafka.internal.utils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.processor.TaskId;

public class TopologyTaskInfo {
  private final Map<TopicPartition, TaskId> tasksByPartition;
  private final Map<TaskId, List<TopicPartition>> partitionsByTask;
  // todo: add info about internal topics

  @VisibleForTesting
  TopologyTaskInfo(
      final Map<TopicPartition, TaskId> tasksByPartition,
      final Map<TaskId, List<TopicPartition>> partitionsByTask
  ) {
    this.tasksByPartition = Map.copyOf(tasksByPartition);
    this.partitionsByTask = Map.copyOf(partitionsByTask);
  }

  public Map<TopicPartition, TaskId> tasksByPartition() {
    return tasksByPartition;
  }

  public Map<TaskId, List<TopicPartition>> partitionsByTask() {
    return partitionsByTask;
  }

  public static TopologyTaskInfo forTopology(
      final TopologyDescription topology,
      final Admin admin
  ) {
    final Map<TopicPartition, TaskId> tasksByPartition = new HashMap<>();
    final Map<TaskId, List<TopicPartition>> partitionsByTask = new HashMap<>();
    final Set<String> sinkTopics = sinkTopics(topology);
    for (final var st : topology.subtopologies()) {
      final Set<String> topics = new HashSet<>();
      for (final TopologyDescription.Node node : st.nodes()) {
        if (node instanceof TopologyDescription.Source) {
          topics.addAll(((TopologyDescription.Source) node).topicSet());
          if (((TopologyDescription.Source) node).topicPattern() != null) {
            throw new TopologyTaskInfoException(
                "topic patterns are not supported for snapshots");
          }
        }
      }
      if (!Sets.intersection(topics, sinkTopics).isEmpty()) {
        throw new TopologyTaskInfoException(
            "internal topics are not supported for snapshots"
        );
      }
      final Map<String, TopicDescription> descriptions;
      try {
        descriptions = admin.describeTopics(topics).allTopicNames().get();
      } catch (final ExecutionException | InterruptedException e) {
        throw new RuntimeException(e);
      }
      final Set<Integer> partitionCounts = descriptions.values().stream()
          .map(d -> d.partitions().size())
          .collect(Collectors.toSet());
      if (partitionCounts.size() != 1) {
        throw new TopologyTaskInfoException(
            "unexpected topics with different partition counts");
      }
      final int nPartitions = partitionCounts.iterator().next();
      for (int i = 0; i < nPartitions; i++) {
        final var taskId = new TaskId(st.id(), i);
        partitionsByTask.put(taskId, new ArrayList<>(nPartitions));
        for (final var topic : topics) {
          final var tp = new TopicPartition(topic, i);
          tasksByPartition.put(tp, taskId);
          partitionsByTask.get(taskId).add(tp);
        }
      }
    }
    return new TopologyTaskInfo(tasksByPartition, partitionsByTask);
  }

  private static Set<String> sinkTopics(TopologyDescription topology) {
    final Set<String> sinkTopics = new HashSet<>();
    for (final var st : topology.subtopologies()) {
      for (final TopologyDescription.Node node : st.nodes()) {
        if (node instanceof TopologyDescription.Sink) {
          final String sinkTopic = ((TopologyDescription.Sink) node).topic();
          if (sinkTopic == null) {
            throw new TopologyTaskInfoException("non-explicit sink topics not yet supported");
          }
          sinkTopics.add(sinkTopic);
        }
      }
    }
    return sinkTopics;
  }

  public static class TopologyTaskInfoException extends RuntimeException {
    private static final long serialVersionUID = 0L;

    public TopologyTaskInfoException(final String message) {
      super(message);
    }
  }
}
