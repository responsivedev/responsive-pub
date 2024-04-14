/*
 * Copyright 2024 Responsive Computing, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package dev.responsive.kafka.api.async.internals.contexts;

import static dev.responsive.kafka.api.async.internals.AsyncThreadPool.ASYNC_THREAD_NAME;
import static dev.responsive.kafka.api.async.internals.AsyncUtils.isAsyncThread;
import static dev.responsive.kafka.api.async.internals.AsyncUtils.isStreamThread;
import static dev.responsive.kafka.api.async.internals.AsyncUtils.isStreamThreadOrAsyncThread;

import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

public class AsyncProcessorContext<KOut, VOut>
    extends DelegatingProcessorContext<KOut, VOut, MergedProcessorContext<KOut, VOut>> {

  private final Logger log;
  private final String streamThreadName;

  private final ThreadLocal<MergedProcessorContext<KOut, VOut>> threadLocalDelegate =
      new ThreadLocal<>();

  public AsyncProcessorContext(
      final String streamThreadName,
      final String logPrefix
  ) {
    this.streamThreadName = streamThreadName;
    this.log = new LogContext(logPrefix).logger(AsyncProcessorContext.class);
  }

  @Override
  public MergedProcessorContext<KOut, VOut> delegate() {
    final var threadLocalContext = threadLocalDelegate.get();

    // Unfortunately the ThreadLocal's #isPresent method is only package-private
    if (threadLocalContext == null) {
      final String threadName = Thread.currentThread().getName();
      if (isStreamThreadOrAsyncThread(threadName, streamThreadName)) {
        log.error("Thread {} attempted to access the context but it was not initialized",
                  threadName);
        throw new IllegalStateException("Uninitialized thread requested context access");
      } else {
        log.error("Unexpected thread type attempted to access the context. Thread name: {}",
                  threadName);
        throw new UnsupportedOperationException("Illegal external thread requested context access");
      }
    }

    return threadLocalContext;
  }

  public void setDelegateForStreamThread(final StreamThreadProcessorContext<KOut, VOut> delegate) {
    final String threadName = Thread.currentThread().getName();
    if (!isStreamThread(threadName, streamThreadName)) {
      log.error("Attempted to set StreamThread context but thread name was {}", threadName);
      throw new IllegalStateException("Incorrect thread initializing StreamThread context");
    }
    threadLocalDelegate.set(delegate);
  }

  public void setDelegateForAsyncThread(final AsyncThreadProcessorContext<KOut, VOut> delegate) {
    final String threadName = Thread.currentThread().getName();
    if (!isAsyncThread(threadName, streamThreadName)) {
      log.error("Attempted to set AsyncThread context but thread name was {}", threadName);
      throw new IllegalStateException("Incorrect thread initializing AsyncThread context");
    }
    threadLocalDelegate.set(delegate);
  }

}
