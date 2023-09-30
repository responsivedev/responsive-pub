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

package dev.responsive.internal.stores;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;

import dev.responsive.api.stores.ResponsiveKeyValueParams;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.Task.TaskType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class ResponsiveStoreTest {

  private static final String NAME = "foo";
  private final ResponsiveStore store =
      new ResponsiveStore(ResponsiveKeyValueParams.keyValue(NAME));

  @Mock
  private StateStore root;
  @Mock
  private InternalProcessorContext<?, ?> context;

  //@Test
  public void shouldCreateGlobalStore() {
    // Given:
    Mockito.when(context.taskType()).thenReturn(TaskType.GLOBAL);

    // When:
    try {
      store.init((StateStoreContext) context, root);
    } catch (final Exception ignored) {
      // delegate initialization will fail with mocks, but still be properly set
    }

    // Then:
    assertThat(store.getDelegate(), instanceOf(ResponsiveGlobalStore.class));
  }

  @Test
  public void shouldCreatePartitionedStoreWhenPassedStoreContext() {
    // Given:
    Mockito.when(context.taskType()).thenReturn(TaskType.ACTIVE);

    // When:
    try {
      store.init((StateStoreContext) context, root);
    } catch (final Exception ignored) {
      // delegate initialization will fail with mocks, but still be properly set
    }

    // Then:
    assertThat(store.getDelegate(), instanceOf(ResponsivePartitionedStore.class));
  }

}
