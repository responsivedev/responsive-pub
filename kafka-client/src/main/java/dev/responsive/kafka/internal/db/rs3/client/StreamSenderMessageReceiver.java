package dev.responsive.kafka.internal.db.rs3.client;

import java.util.Objects;
import java.util.concurrent.CompletionStage;

public class StreamSenderMessageReceiver<S, R> {
  private final StreamSender<S> sender;
  private final CompletionStage<R> message;

  public StreamSenderMessageReceiver(
      final StreamSender<S> sender,
      final CompletionStage<R> message) {
    this.sender = Objects.requireNonNull(sender);
    this.message = Objects.requireNonNull(message);
  }

  public StreamSender<S> sender() {
    return sender;
  }

  public CompletionStage<R> receiver() {
    return message;
  }
}
