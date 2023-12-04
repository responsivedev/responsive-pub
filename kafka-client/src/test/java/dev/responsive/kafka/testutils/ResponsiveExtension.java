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

package dev.responsive.kafka.testutils;

import static dev.responsive.kafka.api.config.ResponsiveConfig.REMOTE_TABLE_CHECK_INTERVAL_MS_CONFIG;
import static dev.responsive.kafka.api.config.ResponsiveConfig.STORAGE_BACKEND_TYPE_CONFIG;
import static dev.responsive.kafka.api.config.ResponsiveConfig.STORAGE_DATACENTER_CONFIG;
import static dev.responsive.kafka.api.config.ResponsiveConfig.STORAGE_DESIRED_NUM_PARTITION_CONFIG;
import static dev.responsive.kafka.api.config.ResponsiveConfig.STORAGE_HOSTNAME_CONFIG;
import static dev.responsive.kafka.api.config.ResponsiveConfig.STORAGE_PORT_CONFIG;
import static dev.responsive.kafka.api.config.ResponsiveConfig.TASK_ASSIGNOR_CLASS_OVERRIDE;
import static dev.responsive.kafka.api.config.ResponsiveConfig.TENANT_ID_CONFIG;
import static dev.responsive.kafka.api.config.ResponsiveConfig.loggedConfig;
import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.InternalConfig.INTERNAL_TASK_ASSIGNOR_CLASS;

import dev.responsive.kafka.api.config.ResponsiveConfig;
import dev.responsive.kafka.api.config.StorageBackend;
import java.lang.reflect.Parameter;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.admin.Admin;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.MongoDBContainer;

public class ResponsiveExtension implements BeforeAllCallback, AfterAllCallback,
    ParameterResolver {

  public static CassandraContainer<?> cassandra = new CassandraContainer<>(TestConstants.CASSANDRA)
      .withInitScript("CassandraDockerInit.cql")
      .withReuse(true);
  public static KafkaContainer kafka = new KafkaContainer(TestConstants.KAFKA)
      .withEnv("KAFKA_GROUP_MIN_SESSION_TIMEOUT_MS", "1000")
      .withEnv("KAFKA_GROUP_MAX_SESSION_TIMEOUT_MS", "60000")
      .withReuse(true);
  public static MongoDBContainer mongo = new MongoDBContainer(TestConstants.MONGODB);

  public static Admin admin;

  public StorageBackend backend = StorageBackend.CASSANDRA;

  public ResponsiveExtension() {
  }

  public ResponsiveExtension(final StorageBackend backend) {
    this.backend = backend;
  }

  @Override
  public void beforeAll(final ExtensionContext context) throws Exception {
    switch (backend) {
      case CASSANDRA:
        cassandra.start();
        break;
      case MONGO_DB:
        mongo.start();
        break;
      case OTTER_POCKET:
        break;
      default:
        throw new IllegalStateException("Unexpected value: " + backend);
    }

    kafka.start();
    admin = Admin.create(Map.of(BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()));
  }

  @Override
  public void afterAll(final ExtensionContext context) throws Exception {
    cassandra.stop();
    kafka.stop();
    admin.close();
  }

  @Override
  public boolean supportsParameter(
      final ParameterContext parameterContext,
      final ExtensionContext extensionContext
  ) throws ParameterResolutionException {
    return parameterContext.getParameter().getType().equals(CassandraContainer.class)
        || parameterContext.getParameter().getType().equals(KafkaContainer.class)
        || parameterContext.getParameter().getType().equals(Admin.class)
        || isContainerConfig(parameterContext);
  }

  @Override
  public Object resolveParameter(
      final ParameterContext parameterContext,
      final ExtensionContext extensionContext
  ) throws ParameterResolutionException {
    if (parameterContext.getParameter().getType() == CassandraContainer.class) {
      return cassandra;
    } else if (parameterContext.getParameter().getType() == KafkaContainer.class) {
      return kafka;
    } else if (parameterContext.getParameter().getType() == Admin.class) {
      return admin;
    } else if (isContainerConfig(parameterContext)) {
      final Map<String, Object> map = new HashMap<>(Map.of(
          TENANT_ID_CONFIG, "responsive_clients",
          INTERNAL_TASK_ASSIGNOR_CLASS, TASK_ASSIGNOR_CLASS_OVERRIDE,
          BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
          STORAGE_DESIRED_NUM_PARTITION_CONFIG, -1,
          REMOTE_TABLE_CHECK_INTERVAL_MS_CONFIG, 100
      ));

      switch (backend) {
        case CASSANDRA:
          map.put(STORAGE_BACKEND_TYPE_CONFIG, StorageBackend.CASSANDRA.name());
          map.put(STORAGE_HOSTNAME_CONFIG, cassandra.getContactPoint().getHostName());
          map.put(STORAGE_PORT_CONFIG, cassandra.getContactPoint().getPort());
          map.put(STORAGE_DATACENTER_CONFIG, cassandra.getLocalDatacenter());
          break;
        case MONGO_DB:
          map.put(STORAGE_BACKEND_TYPE_CONFIG, StorageBackend.MONGO_DB.name());
          map.put(STORAGE_HOSTNAME_CONFIG, mongo.getConnectionString());
          break;
        case OTTER_POCKET:
          map.put(STORAGE_BACKEND_TYPE_CONFIG, StorageBackend.OTTER_POCKET.name());
          map.put(STORAGE_HOSTNAME_CONFIG, "localhost:4242");
          break;
        default:
          throw new IllegalStateException("Unexpected value: " + backend);
      }

      if (parameterContext.getParameter().getType().equals(Map.class)) {
        return map;
      } else  {
        return loggedConfig(map);
      }
    }

    throw new IllegalArgumentException("Unexpected parameter " + parameterContext);
  }

  private static boolean isContainerConfig(final ParameterContext context) {
    final Parameter param = context.getParameter();
    return (param.getType().equals(Map.class) || param.getType().equals(ResponsiveConfig.class))
        && param.getAnnotation(ResponsiveConfigParam.class) != null;
  }
}
