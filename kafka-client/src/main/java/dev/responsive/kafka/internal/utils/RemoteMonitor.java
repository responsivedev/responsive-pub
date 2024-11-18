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

package dev.responsive.kafka.internal.utils;

import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;
import javax.annotation.concurrent.GuardedBy;

/**
 * {@code RemoteMonitor} is a synchronization barrier similar to
 * a {@link Condition} variable that allows multiple watchers to
 * wait on a condition that requires polling on the status of an
 * external resource. This is useful to avoid making the same RPC
 * from multiple callers.
 */
public class RemoteMonitor {

  private final ReentrantLock lock = new ReentrantLock();
  private final Condition satisfied = lock.newCondition();

  @GuardedBy("lock")
  private boolean isSatisfied = false;

  public RemoteMonitor(
      final ScheduledExecutorService executor,
      final BooleanSupplier condition
  ) {
    this(executor, condition, Duration.ofSeconds(1));
  }

  public RemoteMonitor(
      final ScheduledExecutorService executor,
      final BooleanSupplier condition,
      final Duration checkRate
  ) {
    executor.scheduleAtFixedRate(
        () -> signalWhenDone(condition), 0, checkRate.toMillis(), TimeUnit.MILLISECONDS
    );
  }

  public void await(final Duration timeout) throws TimeoutException, InterruptedException {
    lock.lock();
    try {
      while (!isSatisfied) {
        if (!satisfied.await(timeout.toMillis(), TimeUnit.MILLISECONDS) && !isSatisfied) {
          throw new TimeoutException();
        }
      }
    } finally {
      lock.unlock();
    }
  }

  private void signalWhenDone(final BooleanSupplier condition) {
    lock.lock();
    try {
      // avoid calling the condition which may have an RPC if it
      // has already been satisfied
      this.isSatisfied = this.isSatisfied || condition.getAsBoolean();

      if (this.isSatisfied) {
        satisfied.signalAll();
      }
    } finally {
      lock.unlock();
    }
  }
}