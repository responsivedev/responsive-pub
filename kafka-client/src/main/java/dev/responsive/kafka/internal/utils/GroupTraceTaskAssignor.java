package dev.responsive.kafka.internal.utils;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.streams.processor.assignment.ApplicationState;
import org.apache.kafka.streams.processor.assignment.TaskAssignor;
import org.apache.kafka.streams.processor.assignment.assignors.StickyTaskAssignor;

public class GroupTraceTaskAssignor implements TaskAssignor {
  public static final String GROUP_TRACE_ROOT_CONFIG = "__responsive.group.trace.root";

  private final TaskAssignor delegate = new StickyTaskAssignor();
  private GroupTraceRoot groupTraceRoot;

  @Override
  public TaskAssignment assign(final ApplicationState applicationState) {
    return delegate.assign(applicationState);
  }

  @Override
  public void onAssignmentComputed(
      final ConsumerPartitionAssignor.GroupAssignment assignment,
      final ConsumerPartitionAssignor.GroupSubscription subscription,
      final AssignmentError error
  ) {
    final int generation = generation(subscription);
    groupTraceRoot.refresh(generation);
    final var assignSpan = groupTraceRoot.rootSpan().childSpan("assignment");
    final var otelSpan = assignSpan.span();
    for (final var clientToPartition : assignment.groupAssignment().entrySet()) {
      for (final var p : clientToPartition.getValue().partitions()) {
        otelSpan.addEvent(
            "partition-assignment",
            Attributes.of(
                AttributeKey.stringKey("partition"), p.toString(),
                AttributeKey.stringKey("client"), clientToPartition.getKey()
            )
        );
      }
    }
    assignSpan.end();
  }

  private int generation(final ConsumerPartitionAssignor.GroupSubscription subscription) {
    return subscription.groupSubscription().values().stream()
        .map(s -> s.generationId().orElse(-1))
        .max(Integer::compareTo)
        .orElse(-1)
        + 1;
  }

  @Override
  public void configure(final Map<String, ?> configs) {
    this.groupTraceRoot = (GroupTraceRoot) configs.get(GROUP_TRACE_ROOT_CONFIG);
    delegate.configure(configs);
  }
}
