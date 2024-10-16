package dev.responsive.kafka.internal.tracing.otel;

import dev.responsive.kafka.internal.tracing.ResponsiveSpan;
import dev.responsive.kafka.internal.tracing.ResponsiveTracer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.slf4j.Logger;

public class ResponsiveOtelSpan implements ResponsiveSpan {
  private final Logger log;
  private final Map<String, String> tags;
  private final Span span;

  ResponsiveOtelSpan(
      final Logger log,
      final Map<String, String> tags,
      final Span span
  ) {
    this.log = Objects.requireNonNull(log);
    this.tags = Objects.requireNonNull(tags);
    this.span = Objects.requireNonNull(span);
    tags.entrySet().forEach(e -> span.setAttribute(e.getKey(), e.getValue()));
  }

  @Override
  public ResponsiveTracer.Scope makeCurrent() {
    final var beforeAttach = ResponsiveOtelTracer.currentContext();
    final var afterAttach = beforeAttach.with(span);
    ResponsiveOtelTracer.setContext(afterAttach);
    return new ResponsiveOtelTracer.Scope(beforeAttach, afterAttach);
  }

  @Override
  public void end(final String message) {
    log.info(formatLogMessage(message));
    span.setAttribute("message", message);
    span.end();
  }

  @Override
  public void end(final String message, final Throwable e) {
    log.error(formatLogMessage(message), e);
    span.setAttribute("message", message + ": " + e.getMessage());
    span.recordException(e);
    span.setStatus(StatusCode.ERROR);
    span.end();
  }

  private String formatLogMessage(final String message) {
    return String.format(
        "%s - %s",
        tags.entrySet().stream()
            .map(e -> String.format("%s=%s", e.getKey(), e.getValue()))
            .collect(Collectors.joining(", ")),
        message
    );
  }
}
