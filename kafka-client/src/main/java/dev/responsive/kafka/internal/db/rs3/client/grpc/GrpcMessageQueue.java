/*
 * Copyright 2025 Responsive Computing, Inc.
 *
 * This source code is licensed under the Responsive Business Source License Agreement v1.0
 * available at:
 *
 * https://www.responsive.dev/legal/responsive-bsl-10
 *
 * This software requires a valid Commercial License Key for production use. Trial and commercial
 * licenses can be obtained at https://www.responsive.dev
 */

package dev.responsive.kafka.internal.db.rs3.client.grpc;

import dev.responsive.kafka.internal.db.rs3.client.RS3Exception;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

public class GrpcMessageQueue<T> {
  private final BlockingQueue<T> queue = new LinkedBlockingQueue<>();
  private final AtomicReference<T> next = new AtomicReference<>();

  void put(T message) {
    try {
      queue.put(message);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RS3Exception(e);
    }
  }

  T poll() {
    var next = this.next.getAndSet(null);
    if (next == null) {
      next = takeFromQueue();
    }
    return next;
  }

  T peek() {
    var next = this.next.get();
    if (next == null) {
      next = takeFromQueue();
      this.next.set(next);
    }
    return next;
  }

  private T takeFromQueue() {
    try {
      return queue.take();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RS3Exception(e);
    }
  }

}
