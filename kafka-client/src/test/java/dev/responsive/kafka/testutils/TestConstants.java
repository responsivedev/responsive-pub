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

import org.testcontainers.utility.DockerImageName;

public class TestConstants {

  public static final DockerImageName CASSANDRA = DockerImageName.parse("cassandra:4.1.0");
  public static final DockerImageName KAFKA = DockerImageName.parse("confluentinc/cp-kafka:7.9.0");
  public static final DockerImageName MONGODB = DockerImageName.parse("mongo:7.0.2");

}