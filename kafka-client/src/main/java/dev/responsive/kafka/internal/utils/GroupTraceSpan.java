package dev.responsive.kafka.internal.utils;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Context;
import java.util.Objects;

public class GroupTraceSpan {
  final Span span;
  final int generation;
  final String groupName;

  GroupTraceSpan(final Span span, final String groupName, final int generation) {
    this.span = Objects.requireNonNull(span);
    this.generation = generation;
    this.groupName = groupName;
    this.span.setAttribute("group", groupName);
    this.span.setAttribute("generation", generation);
  }

  public GroupTraceSpan childSpan(final String name) {
    final var tracer = GlobalOpenTelemetry.getTracer("responsive");
    final var childSpan = tracer.spanBuilder(name)
        .setParent(Context.root().with(span))
        .startSpan();
    return new GroupTraceSpan(childSpan, groupName, generation);
  }

  public Span span() {
    return span;
  }

  public void end() {
    span.end();
  }
}
