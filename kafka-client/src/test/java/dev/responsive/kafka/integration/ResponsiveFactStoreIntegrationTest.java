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

package dev.responsive.kafka.integration;

import static dev.responsive.kafka.api.config.ResponsiveConfig.STORE_FLUSH_RECORDS_TRIGGER_CONFIG;
import static dev.responsive.kafka.testutils.IntegrationTestUtils.getCassandraValidName;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

import dev.responsive.kafka.api.config.ResponsiveConfig;
import dev.responsive.kafka.api.stores.ResponsiveKeyValueParams;
import dev.responsive.kafka.internal.config.InternalConfigs;
import dev.responsive.kafka.internal.db.CassandraClient;
import dev.responsive.kafka.internal.db.CassandraClientFactory;
import dev.responsive.kafka.internal.db.DefaultCassandraClientFactory;
import dev.responsive.kafka.internal.stores.ResponsiveFactStore;
import dev.responsive.kafka.internal.stores.ResponsiveStoreRegistry;
import dev.responsive.kafka.testutils.ResponsiveConfigParam;
import dev.responsive.kafka.testutils.ResponsiveExtension;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith({ResponsiveExtension.class, MockitoExtension.class})
class ResponsiveFactStoreIntegrationTest {

  private final Map<String, Object> responsiveProps = new HashMap<>();

  @Mock
  private InternalProcessorContext<?, ?> storeContext;
  @Mock
  private StateStore root;

  private ResponsiveStoreRegistry registry;
  private String name;
  private CassandraClient client;

  @BeforeEach
  public void before(
      final TestInfo info,
      final Admin admin,
      @ResponsiveConfigParam final Map<String, Object> responsiveProps
  ) {
    this.name = getCassandraValidName(info);

    final CassandraClientFactory defaultFactory = new DefaultCassandraClientFactory();
    final ResponsiveConfig config = ResponsiveConfig.responsiveConfig(responsiveProps);
    client = defaultFactory.createCassandraClient(
        defaultFactory.createCqlSession(config),
        config);
    registry = new ResponsiveStoreRegistry();

    this.responsiveProps.put(STORE_FLUSH_RECORDS_TRIGGER_CONFIG, 1);
    this.responsiveProps.putAll(responsiveProps);
    this.responsiveProps.putAll(new InternalConfigs.Builder()
        .withKafkaAdmin(admin)
        .withCassandraClient(client)
        .withStoreRegistry(registry)
        .build());

    when(storeContext.taskId()).thenReturn(new TaskId(0, 0));
    when(storeContext.appConfigs()).thenReturn(this.responsiveProps);
    when(storeContext.applicationId()).thenReturn("appId");
    when(storeContext.changelogFor(name)).thenReturn(name + "-changelog");
  }

  @Test
  public void shouldSupportPutIfAbsent() {
    // Given:
    final var params = ResponsiveKeyValueParams.fact(name);
    final var store = new ResponsiveFactStore(params);

    store.init((StateStoreContext) storeContext, root);

    // When:
    final byte[] r1 = store.putIfAbsent(Bytes.wrap(new byte[] {1}), new byte[] {1});
    final byte[] r2 = store.putIfAbsent(Bytes.wrap(new byte[] {1}), new byte[] {2});

    registry.stores().get(0).onCommit().accept(1L); // trigger flush to remote

    final byte[] r3 = store.putIfAbsent(Bytes.wrap(new byte[] {1}), new byte[] {3});
    final byte[] r4 = store.putIfAbsent(Bytes.wrap(new byte[] {2}), new byte[] {1});

    // Then:
    assertThat(r1, nullValue());
    assertThat(r2, is(new byte[]{1}));
    assertThat(r3, is(new byte[]{1}));
    assertThat(r4, nullValue());
    assertThat(client.execute("SELECT * FROM " + name + ";").all().size(), is(1));
  }

  @Test
  public void shouldFailOnPut() {
    // Given:
    final var params = ResponsiveKeyValueParams.fact(name);
    final var store = new ResponsiveFactStore(params);

    store.init((StateStoreContext) storeContext, root);

    // Then:
    assertThrows(
        UnsupportedOperationException.class,
        () -> store.put(Bytes.wrap(new byte[] {1}), new byte[] {1})
    );
  }

  @Test
  public void shouldFailOnPutAll() {
    // Given:
    final var params = ResponsiveKeyValueParams.fact(name);
    final var store = new ResponsiveFactStore(params);

    store.init((StateStoreContext) storeContext, root);

    // Then:
    assertThrows(
        UnsupportedOperationException.class,
        () -> store.putAll(List.of(new KeyValue<>(Bytes.wrap(new byte[] {1}), new byte[] {1})))
    );
  }

  @Test
  public void shouldFailOnDelete() {
    // Given:
    final var params = ResponsiveKeyValueParams.fact(name);
    final var store = new ResponsiveFactStore(params);

    store.init((StateStoreContext) storeContext, root);

    // Then:
    assertThrows(
        UnsupportedOperationException.class,
        () -> store.delete(Bytes.wrap(new byte[] {1}))
    );
  }

}