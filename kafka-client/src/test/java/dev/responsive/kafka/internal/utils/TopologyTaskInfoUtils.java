package dev.responsive.kafka.internal.utils;

import java.util.List;
import java.util.Map;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.processor.TaskId;

public final class TopologyTaskInfoUtils {
  public static TopologyTaskInfo createWith(
      final Map<TopicPartition, TaskId> tasksByPartition,
      final Map<TaskId, List<TopicPartition>> partitionsByTask
  ) {
    return new TopologyTaskInfo(tasksByPartition, partitionsByTask);
  }

  private TopologyTaskInfoUtils() {
  }
}
