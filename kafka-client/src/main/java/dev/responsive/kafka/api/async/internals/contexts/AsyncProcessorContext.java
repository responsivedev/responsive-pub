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

public class AsyncProcessorContext<KOut, VOut>
    extends DelegatingProcessorContext<KOut, VOut, MergedProcessorContext<KOut, VOut>> {

  private final ThreadLocal<MergedProcessorContext<KOut, VOut>> threadLocalDelegate =
      new ThreadLocal<>();

  @Override
  public MergedProcessorContext<KOut, VOut> delegate() {
    // todo: assert this is only called from stream thread or async processor thread
    return threadLocalDelegate.get();
  }

  public void setDelegateForCurrentThread(final MergedProcessorContext<KOut, VOut> delegate) {
    threadLocalDelegate.set(delegate);
  }
}
