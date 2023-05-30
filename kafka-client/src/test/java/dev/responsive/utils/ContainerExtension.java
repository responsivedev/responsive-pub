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

  @Override
  public void beforeAll(final ExtensionContext context) throws Exception {
    cassandra.start();
    kafka.start();
  }

  @Override
  public void afterAll(final ExtensionContext context) throws Exception {
    cassandra.stop();
    kafka.stop();
  }

  @Override
  public boolean supportsParameter(
      final ParameterContext parameterContext,
      final ExtensionContext extensionContext
  ) throws ParameterResolutionException {
    return parameterContext.getParameter().getType().equals(CassandraContainer.class)
        || parameterContext.getParameter().getType().equals(KafkaContainer.class);
  }

  @Override
  public Object resolveParameter(
      final ParameterContext parameterContext,
      final ExtensionContext extensionContext
  ) throws ParameterResolutionException {
    return parameterContext.getParameter().getType() == CassandraContainer.class
        ? cassandra : kafka;
  }
}
