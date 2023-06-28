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

package dev.responsive.kafka.api;

import dev.responsive.db.CassandraClient;
import dev.responsive.kafka.store.ResponsiveStore;
import dev.responsive.utils.RemoteMonitor;
import dev.responsive.utils.TableName;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;

public class ResponsiveKeyValueBytesStoreSupplier implements KeyValueBytesStoreSupplier {

  private final CassandraClient client;
  private final TableName name;
  private final RemoteMonitor awaitTable;
  private final Admin admin;

  public ResponsiveKeyValueBytesStoreSupplier(
      final CassandraClient client,
      final String name,
      final ScheduledExecutorService executorService,
      final Admin admin
  ) {
    this.client = client;
    this.name = new TableName(name);
    this.admin = admin;
    awaitTable = client.awaitTable(this.name.cassandraName(), executorService);
  }

  @Override
  public String name() {
    return name.kafkaName();
  }

  @Override
  public KeyValueStore<Bytes, byte[]> get() {
    return new ResponsiveStore(client, name, awaitTable, admin);
  }

  @Override
  public String metricsScope() {
    return "responsive";
  }
}