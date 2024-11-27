/*
 * Copyright 2024 Responsive Computing, Inc.
 *
 * This source code is licensed under the Responsive Business Source License Agreement v1.0
 * available at:
 *
 * https://www.responsive.dev/legal/responsive-bsl-10
 *
 * This software requires a valid Commercial License Key for production use. Trial and commercial
 * licenses can be obtained at https://www.responsive.dev
 */

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
