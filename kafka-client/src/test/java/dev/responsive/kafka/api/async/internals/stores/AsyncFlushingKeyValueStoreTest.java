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

package dev.responsive.kafka.api.async.internals.stores;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class AsyncFlushingKeyValueStoreTest {
  private static final TaskId TASK_ID = new TaskId(0, 3);

  @Mock
  private KeyValueStore<Bytes, byte[]> wrapped;
  @Mock
  private StreamThreadFlushListeners flushListeners;
  @Mock
  private StateStoreContext storeContext;
  @Mock
  private StateStore rootStore;

  private AsyncFlushingKeyValueStore asyncFlushingKeyValueStore;

  @BeforeEach
  public void setUp() {
    when(storeContext.taskId()).thenReturn(TASK_ID);
    asyncFlushingKeyValueStore = new AsyncFlushingKeyValueStore(wrapped, flushListeners);
  }

  @Test
  public void shouldDeregisterFlushListenerIfWrappedInitThrows() {
    // given:
    doThrow(new RuntimeException("oops")).when(wrapped).init(
        any(StateStoreContext.class),
        any(StateStore.class)
    );

    // when:
    try {
      asyncFlushingKeyValueStore.init(storeContext, rootStore);
    } catch (RuntimeException e) {
      // ignore
    }

    verify(flushListeners).unregisterListenerForPartition(TASK_ID.partition());
  }
}