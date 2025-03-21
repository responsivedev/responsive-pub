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

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;

public class GrpcMessageQueue<T> {
  private final ConcurrentLinkedQueue<T> queue = new ConcurrentLinkedQueue<>();
  private final Semaphore semaphore = new Semaphore(0);
  private final AtomicReference<Sync> sync =
      new AtomicReference<>(new Sync(ReaderState.IDLE, 0, 0));

  void put(T message) {
    queue.offer(message);
    final var updated = sync.updateAndGet(current -> {
      final ReaderState updatedReaderState;
      if (current.readerState == ReaderState.BLOCKED) {
        updatedReaderState = ReaderState.UNBLOCKED;
      } else {
        updatedReaderState = current.readerState;
      }
      return new Sync(updatedReaderState, current.seq + 1, current.ack);
    });
    if (updated.readerState == ReaderState.UNBLOCKED) {
      semaphore.release();
    }
  }

  T poll() {
    while (true) {
      final var message = queue.poll();
      if (message == null) {
        blockUntilNonEmpty();
      } else {
        sync.updateAndGet(current -> new Sync(ReaderState.IDLE, current.seq, current.ack + 1));
        return message;
      }
    }
  }

  T peek() {
    while (true) {
      final var message = queue.peek();
      if (message == null) {
        blockUntilNonEmpty();
      } else {
        return message;
      }
    }
  }

  void blockUntilNonEmpty() {
    final var update = sync.updateAndGet(current -> {
      final ReaderState updatedReaderState;
      if (current.seq == current.ack) {
        updatedReaderState = ReaderState.BLOCKED;
      } else {
        updatedReaderState = current.readerState;
      }
      return new Sync(updatedReaderState, current.seq, current.ack);
    });
    if (update.readerState == ReaderState.BLOCKED) {
      semaphore.acquireUninterruptibly();
    }
  }

  enum ReaderState {
    IDLE, BLOCKED, UNBLOCKED
  }

  static final class Sync {
    final ReaderState readerState;
    final long seq;
    final long ack;

    Sync(ReaderState readerState, long seq, long ack) {
      this.readerState = readerState;
      this.seq = seq;
      this.ack = ack;
    }
  }
}
