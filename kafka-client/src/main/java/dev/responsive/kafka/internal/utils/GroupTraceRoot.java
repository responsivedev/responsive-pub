package dev.responsive.kafka.internal.utils;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.context.Context;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;
import java.util.Random;
import org.apache.commons.codec.binary.Hex;

public class GroupTraceRoot {
  public static ThreadLocal<GroupTraceRoot> INSTANCE = new ThreadLocal<>();

  private int generation = -1;
  private GroupTraceSpan currentSpan;
  private final String groupName;
  private final Random random = new Random();

  public static GroupTraceRoot create(final String groupName) {
    return new GroupTraceRoot(groupName, -1);
  }

  public void refresh(final int generation) {
    if (generation <= this.generation) {
      return;
    }
    this.generation = generation;
    if (currentSpan != null) {
      currentSpan.end();
    }
    final var traceId = groupTraceId(groupName, generation);
    final var tracer = GlobalOpenTelemetry.getTracer("responsive");
    final SpanContext spanContext = SpanContext.createFromRemoteParent(
        traceId,
        randomSpanId(),
        TraceFlags.getSampled(),
        TraceState.getDefault()
    );
    final Context context = Context.root().with(Span.wrap(spanContext));
    currentSpan = new GroupTraceSpan(
        tracer.spanBuilder("consumer-group-root")
            .setParent(context)
            .startSpan(),
        groupName,
        generation
    );
  }

  public GroupTraceSpan rootSpan() {
    return currentSpan;
  }

  private String randomSpanId() {
    final long idAsLong = random.nextLong();
    final ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
    buffer.putLong(idAsLong);
    final var spanId = Hex.encodeHexString(buffer.array());
    assert spanId.length() == 16;
    return spanId;
  }

  private static String groupTraceId(final String groupName, final int generation) {
    final ByteBuffer buffer = ByteBuffer.allocate(groupName.length() + Integer.BYTES);
    buffer.put(groupName.getBytes(StandardCharsets.UTF_8));
    buffer.putInt(generation);
    final byte[] bytes = buffer.array();
    final MessageDigest md5;
    try {
      md5 = MessageDigest.getInstance("MD5");
    } catch (final NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
    final byte[] digest = md5.digest(bytes);
    final var traceId = Hex.encodeHexString(digest);
    assert traceId.length() == 32;
    return traceId;
  }

  private GroupTraceRoot(final String groupName, final int generation) {
    this.groupName = Objects.requireNonNull(groupName);
    this.generation = generation;
  }
}
