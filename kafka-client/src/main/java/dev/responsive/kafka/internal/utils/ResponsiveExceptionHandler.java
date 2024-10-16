package dev.responsive.kafka.internal.utils;

import dev.responsive.kafka.internal.tracing.ResponsiveTracer;
import dev.responsive.kafka.internal.tracing.otel.ResponsiveOtelTracer;
import java.util.Optional;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResponsiveExceptionHandler implements StreamsUncaughtExceptionHandler {
  private static final Logger log = LoggerFactory.getLogger(ResponsiveExceptionHandler.class);
  private final ResponsiveTracer tracer = new ResponsiveOtelTracer(log);

  @Override
  public StreamThreadExceptionResponse handle(final Throwable exception) {
    tracer.error("uncaught-exception", "uncaught exception in stream thread", exception);
    ResponsiveOtelTracer.cleanupStreamThread(Optional.of(exception));
    return StreamThreadExceptionResponse.REPLACE_THREAD;
  }
}
