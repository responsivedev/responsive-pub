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

package dev.responsive.utils;

import static dev.responsive.kafka.config.ResponsiveConfig.STORAGE_DATACENTER_CONFIG;
import static dev.responsive.kafka.config.ResponsiveConfig.STORAGE_HOSTNAME_CONFIG;
import static dev.responsive.kafka.config.ResponsiveConfig.STORAGE_PORT_CONFIG;
import static dev.responsive.kafka.config.ResponsiveConfig.TASK_ASSIGNOR_CLASS_OVERRIDE;
import static dev.responsive.kafka.config.ResponsiveConfig.TENANT_ID_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.InternalConfig.INTERNAL_TASK_ASSIGNOR_CLASS;

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

public class ContainerExtension implements BeforeAllCallback, AfterAllCallback,
    ParameterResolver {

  public static CassandraContainer<?> cassandra = new CassandraContainer<>(TestConstants.CASSANDRA)
      .withInitScript("CassandraDockerInit.cql")
      .withReuse(true);
  public static KafkaContainer kafka = new KafkaContainer(TestConstants.KAFKA)
      .withEnv("KAFKA_GROUP_MIN_SESSION_TIMEOUT_MS", "1000")
      .withEnv("KAFKA_GROUP_MAX_SESSION_TIMEOUT_MS", "60000")
      .withReuse(true);

  public static Admin admin;

  @Override
  public void beforeAll(final ExtensionContext context) throws Exception {
    cassandra.start();
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
      return new HashMap<>(Map.of(
          STORAGE_HOSTNAME_CONFIG, cassandra.getContactPoint().getHostName(),
          STORAGE_PORT_CONFIG, cassandra.getContactPoint().getPort(),
          STORAGE_DATACENTER_CONFIG, cassandra.getLocalDatacenter(),
          TENANT_ID_CONFIG, "responsive_clients",
          INTERNAL_TASK_ASSIGNOR_CLASS, TASK_ASSIGNOR_CLASS_OVERRIDE,
          BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()
      ));
    }

    throw new IllegalArgumentException("Unexpected parameter " + parameterContext);
  }

  private static boolean isContainerConfig(final ParameterContext context) {
    return context.getParameter().getType().equals(Map.class)
        && context.getParameter().getAnnotation(ResponsiveConfigParam.class) != null;
  }
}
