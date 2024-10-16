package dev.responsive.kafka.internal.tracing.otel;


import dev.responsive.kafka.internal.tracing.ResponsiveSpan;
import dev.responsive.kafka.internal.tracing.ResponsiveTracer;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Context;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;

public class ResponsiveOtelTracer implements ResponsiveTracer {
  private final static ThreadLocal<ThreadContext> STREAM_THREAD_CONTEXT = new ThreadLocal<>();

  private final Map<String, String> tags;
  private final Logger log;

  public static void initForStreamThread(final String threadName) {
    if (STREAM_THREAD_CONTEXT.get() != null) {
      return;
    }
    final var tracer = GlobalOpenTelemetry.getTracer("responsive");
    final Span rootSpan = tracer.spanBuilder(threadName)
        .setParent(Context.root())
        .startSpan();
    rootSpan.setAttribute(TAG_THREAD, threadName);
    STREAM_THREAD_CONTEXT.set(new ThreadContext(rootSpan, threadName));
  }

  public static void cleanupStreamThread(final Optional<Throwable> error) {
    final var threadContext = STREAM_THREAD_CONTEXT.get();
    STREAM_THREAD_CONTEXT.remove();
    if (threadContext != null) {
      error.ifPresent(threadContext.rootSpan::recordException);
      threadContext.rootSpan.end();
    }
  }

  public ResponsiveOtelTracer(final Logger log) {
    this(Collections.emptyMap(), log);
  }

  ResponsiveOtelTracer(final Map<String, String> tags, final Logger log) {
    this.tags = new HashMap<>(tags);
    this.log = log;
  }

  @Override
  public ResponsiveTracer withTag(final String key, final String value) {
    tags.put(key, value);
    return this;
  }

  @Override
  public ResponsiveTracer withClientId(String clientId) {
    tags.put(TAG_CLIENT_ID, clientId);
    return this;
  }

  @Override
  public ResponsiveTracer withPartition(final TopicPartition partition) {
    return withTag(ResponsiveTracer.TAG_PARTITION, partition.toString());
  }

  @Override
  public ResponsiveTracer withTask(final String topic) {
    return withTag(ResponsiveTracer.TAG_TASK, topic);
  }

  @Override
  public ResponsiveTracer withThread(final String threadName) {
    return this;
  }

  @Override
  public ResponsiveTracer withStore(final String storeName) {
    return withTag(ResponsiveTracer.TAG_STORE, storeName);
  }

  @Override
  public ResponsiveTracer clone(final Logger log) {
    return new ResponsiveOtelTracer(tags, log);
  }

  @Override
  public ResponsiveSpan span(final String label, final Level level) {
    final var tracer = GlobalOpenTelemetry.getTracer("responsive");
    final var span = tracer.spanBuilder(label)
        .setParent(currentContext())
        .startSpan();
    span.setAttribute(
        TAG_THREAD,
        STREAM_THREAD_CONTEXT.get().threadName
    );
    return new ResponsiveOtelSpan(log, tags, span);
  }

  @Override
  public void info(final String label, final String message) {
    final var span = span(label, Level.INFO);
    span.end(message);
  }

  @Override
  public void debug(final String label, final String message) {
    final var span = span(label, Level.DEBUG);
    span.end(message);
  }

  @Override
  public void error(final String label, final String message) {
    final var span = span(label, Level.ERROR);
    span.end(message);
  }

  @Override
  public void error(final String label, final String message, final Throwable e) {
    final var span = span(label, Level.ERROR);
    span.end(message, e);
  }

  static class Scope implements ResponsiveTracer.Scope {
    @Nullable
    private final Context beforeAttach;
    private final Context toAttach;
    private boolean closed;

    Scope(@Nullable Context beforeAttach, Context toAttach) {
      this.beforeAttach = beforeAttach;
      this.toAttach = toAttach;
    }

    @Override
    public void close() {
      if (!closed && STREAM_THREAD_CONTEXT.get().currentContext == toAttach) {
        closed = true;
        STREAM_THREAD_CONTEXT.get().currentContext = beforeAttach;
      }
    }
  }

  static void setContext(final Context context) {
    STREAM_THREAD_CONTEXT.get().currentContext = context;
  }

  static Context currentContext() {
    final var ctx = STREAM_THREAD_CONTEXT.get();
    if (ctx == null) {
      return Context.root();
    }
    return ctx.currentContext;
  }

  static class ThreadContext {
    private final Span rootSpan;
    private final String threadName;
    private Context currentContext;

    public ThreadContext(Span rootSpan, String threadName) {
      this.rootSpan = rootSpan;
      this.threadName = threadName;
      this.currentContext = Context.root().with(rootSpan);
    }
  }
}
