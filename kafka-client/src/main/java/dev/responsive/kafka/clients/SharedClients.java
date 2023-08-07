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

package dev.responsive.kafka.clients;

import dev.responsive.db.CassandraClient;
import dev.responsive.kafka.api.InternalConfigs;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.kafka.clients.admin.Admin;

/**
 * Basic container class for session clients and other shared resources
 */
public final class SharedClients {
  public final CassandraClient cassandraClient;
  public final Admin admin;
  public final ScheduledExecutorService executor;

  public SharedClients(final Map<String, Object> configs) {
    cassandraClient = InternalConfigs.loadCassandraClient(configs);
    admin = InternalConfigs.loadKafkaAdmin(configs);
    executor = InternalConfigs.loadExecutorService(configs);
  }

}
