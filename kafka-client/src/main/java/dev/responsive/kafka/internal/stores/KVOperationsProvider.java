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

package dev.responsive.kafka.internal.stores;

import dev.responsive.kafka.api.stores.ResponsiveKeyValueParams;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.Task;

@FunctionalInterface
public interface KVOperationsProvider {

  KeyValueOperations provide(
      final ResponsiveKeyValueParams params,
      final Optional<TtlResolver<?, ?>> ttlResolver,
      final StateStoreContext context,
      final Task.TaskType type
  ) throws InterruptedException, TimeoutException;

}
