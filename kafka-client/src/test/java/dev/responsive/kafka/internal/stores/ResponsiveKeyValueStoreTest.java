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
        ResponsiveKeyValueParams.keyValue("license-test"),
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
        ResponsiveKeyValueParams.keyValue("license-test"),
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