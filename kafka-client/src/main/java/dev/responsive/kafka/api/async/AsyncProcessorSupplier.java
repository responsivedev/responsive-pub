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

package dev.responsive.kafka.api.async;

import dev.responsive.kafka.api.async.internals.AsyncProcessor;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;

public final class AsyncProcessorSupplier<KIn, VIn, KOut, VOut> implements ProcessorSupplier<KIn, VIn, KOut, VOut> {

  private final ProcessorSupplier<KIn, VIn, KOut, VOut> userDelegate;
  private final String[] stateStoreNames;

  /**
   * Create an async wrapper around your custom {@link ProcessorSupplier} to enable parallel
   * processing of long/blocking calls and state store accesses. All the usual requirements for
   * the underlying {@link ProcessorSupplier} remain, such as connecting state stores to the
   * processor in your topology -- passing the state store names in here is an additional
   * requirement, not a replacement.
   *
   * @param processorSupplier the {@link ProcessorSupplier} that returns a (new) instance
   *                          of your custom {@link Processor} on each invocation of
   *                          {@link ProcessorSupplier#get}
   * @param stateStoreNames   the name(s) of any state stores that have been connected
   */
  public AsyncProcessorSupplier(
      final ProcessorSupplier<KIn, VIn, KOut, VOut> processorSupplier,
      final String... stateStoreNames
  ) {
    this.userDelegate = processorSupplier;
    this.stateStoreNames = stateStoreNames;
  }

  @Override
  public AsyncProcessor<KIn, VIn, KOut, VOut> get() {
    return new AsyncProcessor<>(userDelegate.get(), stateStoreNames);
  }
}
