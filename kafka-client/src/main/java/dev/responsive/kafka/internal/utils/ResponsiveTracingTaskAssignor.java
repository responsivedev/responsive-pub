package dev.responsive.kafka.internal.utils;

import dev.responsive.kafka.internal.tracing.ResponsiveTracer;
import dev.responsive.kafka.internal.tracing.otel.ResponsiveOtelTracer;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.processor.assignment.ApplicationState;
import org.apache.kafka.streams.processor.assignment.TaskAssignor;
import org.apache.kafka.streams.processor.assignment.assignors.StickyTaskAssignor;
import org.slf4j.LoggerFactory;

public class ResponsiveTracingTaskAssignor implements TaskAssignor {
  private final TaskAssignor delegate = new StickyTaskAssignor();
  private ResponsiveOtelTracer tracer = new ResponsiveOtelTracer(
      LoggerFactory.getLogger(ResponsiveTracingTaskAssignor.class)
  );

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
    final var assignSpan = tracer.span("assignment", ResponsiveTracer.Level.INFO);
    final StringBuilder builder = new StringBuilder();
    for (final var clientToPartition : assignment.groupAssignment().entrySet()) {
      builder.append(String.format(
          "%s -> %s | ",
          clientToPartition.getKey(),
          clientToPartition.getValue().partitions().stream()
              .map(TopicPartition::toString)
              .collect(Collectors.joining(","))
      ));
    }
    assignSpan.end(builder.toString());
  }

  @Override
  public void configure(final Map<String, ?> configs) {
    delegate.configure(configs);
  }
}
