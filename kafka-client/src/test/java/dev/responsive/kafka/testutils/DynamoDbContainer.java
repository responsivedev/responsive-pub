/*
 * Copyright 2024 Responsive Computing, Inc.
 *
 * This source code is licensed under the Responsive Business Source License Agreement v1.0
 * available at:
 *
 * https://www.responsive.dev/legal/responsive-bsl-10
 *
 * This software requires a valid Commercial License Key for production use. Trial and commercial
 * licenses can be obtained at https://www.responsive.dev
 */

package dev.responsive.kafka.testutils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

public class DynamoDbContainer extends GenericContainer<DynamoDbContainer> {
  private static final Logger LOG = LoggerFactory.getLogger(DynamoDbContainer.class);

  public DynamoDbContainer(final DockerImageName dockerImageName) {
    super(dockerImageName);
    this.withExposedPorts(8000);
    this.withCommand("-jar DynamoDBLocal.jar -inMemory -sharedDb");
  }

  public String getConnectionString() {
    return "http://" + getHost() + ":" + getMappedPort(8000);
  }
}
