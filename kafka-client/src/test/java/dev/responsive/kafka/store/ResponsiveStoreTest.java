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

package dev.responsive.kafka.store;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;

import dev.responsive.utils.TableName;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.GlobalProcessorContextImpl;
import org.apache.kafka.streams.processor.internals.ProcessorContextImpl;
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

  private static final TableName NAME = new TableName("foo");

  @Mock
  private StateStore root;

  @Test
  public void shouldCreateGlobalStoreWhenPassedGlobalStoreContext() {
    // Given:
    final StateStoreContext context = Mockito.mock(GlobalProcessorContextImpl.class);
    final var store = new ResponsiveStore(NAME);

    // When:
    try {
      store.init(context, root);
    } catch (final Exception ignored) {
      // delegate initialization will fail with mocks, but still be properly set
    }

    // Then:
    assertThat(store.getDelegate(), instanceOf(ResponsiveGlobalStore.class));
  }

  @Test
  public void shouldCreatePartitionedStoreWhenPassedStoreContext() {
    // Given:
    final StateStoreContext context = Mockito.mock(ProcessorContextImpl.class);
    final var store = new ResponsiveStore(NAME);

    // When:
    try {
      store.init(context, root);
    } catch (final Exception ignored) {
      // delegate initialization will fail with mocks, but still be properly set
    }

    // Then:
    assertThat(store.getDelegate(), instanceOf(ResponsivePartitionedStore.class));
  }

}
