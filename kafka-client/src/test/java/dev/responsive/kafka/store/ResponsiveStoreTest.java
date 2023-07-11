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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import dev.responsive.db.CassandraClient;
import dev.responsive.db.CassandraClient.OffsetRow;
import dev.responsive.kafka.api.InternalConfigs;
import dev.responsive.kafka.config.ResponsiveConfig;
import dev.responsive.utils.RemoteMonitor;
import dev.responsive.utils.TableName;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.MockAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.GlobalProcessorContextImpl;
import org.apache.kafka.streams.processor.internals.ProcessorContextImpl;
import org.junit.jupiter.api.BeforeEach;
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

  private static final TableName NAME = new TableName("table");

  @Mock
  private CassandraClient client;
  @Mock
  private RemoteMonitor monitor;
  @Mock
  private StateStore root;
  @Mock
  private ScheduledExecutorService executor;
  @Mock
  private TopologyDescription description;
  @Mock
  private ResponsiveStoreRegistry registry;

  private Admin admin = MockAdminClient.create().build();

  private Map<String, Object> config;

  @BeforeEach
  public void before() {
    when(client.awaitTable(any(), any())).thenReturn(monitor);
    when(client.getOffset(any(), anyInt())).thenReturn(new OffsetRow(0, null));
    config = new HashMap<>(Map.of(
        StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
        StreamsConfig.APPLICATION_ID_CONFIG, "test",
        StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, StringSerde.class,
        StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, StringSerde.class,
        ResponsiveConfig.TENANT_ID_CONFIG, "foo"
    ));
    config.putAll(
        new InternalConfigs.Builder()
            .withCassandraClient(client)
            .withKafkaAdmin(admin)
            .withExecutorService(executor)
            .withTopologyDescription(description)
            .withStoreRegistry(registry)
            .build()
    );
    admin.createTopics(List.of(
        new NewTopic("app-table-changelog", 1, (short) 1)
    ));
  }

  @Test
  public void shouldCreateGlobalStoreWhenPassedGlobalStoreContext() {
    // Given:
    final StateStoreContext context = Mockito.mock(GlobalProcessorContextImpl.class);
    when(context.applicationId()).thenReturn("app");
    when(context.appConfigs()).thenReturn(config);
    final var store = new ResponsiveStore(NAME);

    // When:
    store.init(context, root);

    // Then:
    assertThat(store.getDelegate(), instanceOf(ResponsiveGlobalStore.class));
    verify(context).register(eq(root), any());
  }

  @Test
  public void shouldCreatePartitionedStoreWhenPassedStoreContext() {
    // Given:
    final StateStoreContext context = Mockito.mock(ProcessorContextImpl.class);
    when(context.applicationId()).thenReturn("app");
    when(context.appConfigs()).thenReturn(config);
    when(context.taskId()).thenReturn(new TaskId(0, 0));
    final var store = new ResponsiveStore(NAME);

    // When:
    store.init(context, root);

    // Then:
    assertThat(store.getDelegate(), instanceOf(ResponsivePartitionedStore.class));
    verify(context).register(eq(root), any());
  }

}
