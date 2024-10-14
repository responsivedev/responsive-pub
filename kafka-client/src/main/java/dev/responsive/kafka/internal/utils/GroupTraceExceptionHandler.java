package dev.responsive.kafka.internal.utils;

import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;

public class GroupTraceExceptionHandler implements StreamsUncaughtExceptionHandler {

  @Override
  public StreamThreadExceptionResponse handle(final Throwable exception) {
    final var span = GroupTraceRoot.INSTANCE.get().rootSpan().childSpan("uncaught-exception");
    span.span().recordException(exception);
    span.end();
    return StreamThreadExceptionResponse.REPLACE_THREAD;
  }
}
