package dev.responsive.kafka.internal.tracing;

public interface ResponsiveSpan {
  ResponsiveTracer.Scope makeCurrent();

  /**
   * Close this span and emit
   */
  void end(String message);

  /**
   * Close this span and emit
   */
  void end(String message, Throwable e);

}
