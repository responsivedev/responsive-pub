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

package dev.responsive.kafka.internal.stores;

import dev.responsive.kafka.api.stores.ResponsiveKeyValueParams;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.Task;

@FunctionalInterface
public interface KVOperationsProvider {

  KeyValueOperations provide(
      final ResponsiveKeyValueParams params,
      final StateStoreContext context,
      final Task.TaskType type
  ) throws InterruptedException, TimeoutException;

}
