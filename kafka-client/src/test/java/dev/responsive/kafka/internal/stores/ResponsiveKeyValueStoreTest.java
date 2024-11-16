/*
 * Copyright 2024 Responsive Computing, Inc.
 *
 * This source code is licensed under the Responsive Software License Agreement v1.0 available at
 *
 * https://www.responsive.dev/legal/responsive-bsl-10
 *
 * This software requires a valid Commercial License Key for production use. Trial and commercial
 * licenses can be obtained at https://www.responsive.dev/sdk/get-started.
 */

package dev.responsive.kafka.internal.stores;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import dev.responsive.kafka.api.stores.ResponsiveKeyValueParams;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.state.internals.MeteredKeyValueStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ResponsiveKeyValueStoreTest {

  @Mock
  private KeyValueOperations ops;
  @Mock
  private InternalProcessorContext<?, ?> context;
  @Mock
  private MeteredKeyValueStore<?, ?> root;

  @Test
  public void shouldPutIfAbsent() {
    // Given:
    final var store = new ResponsiveKeyValueStore(
        ResponsiveKeyValueParams.keyValue("test"),
        (params, ttlResolver, context, type) -> ops
    );
    store.init((StateStoreContext) context, root);
    when(ops.get(any())).thenReturn(null);

    // When:
    store.putIfAbsent(Bytes.wrap(new byte[]{123}), new byte[]{123});

    // Then:
    verify(ops).put(Bytes.wrap(new byte[]{123}), new byte[]{123});
  }

  @Test
  public void shouldNotPutIfAbsentWhenPresent() {
    // Given:
    final var store = new ResponsiveKeyValueStore(
        ResponsiveKeyValueParams.keyValue("test"),
        (params, ttlResolver, context, type) -> ops
    );
    store.init((StateStoreContext) context, root);
    when(ops.get(any())).thenReturn(new byte[]{125});

    // When:
    store.putIfAbsent(Bytes.wrap(new byte[]{123}), new byte[]{123});

    // Then:
    verify(ops, Mockito.never()).put(any(), any());
  }

}