/*
 * Copyright 2023 Responsive Computing, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dev.responsive.internal.utils;

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