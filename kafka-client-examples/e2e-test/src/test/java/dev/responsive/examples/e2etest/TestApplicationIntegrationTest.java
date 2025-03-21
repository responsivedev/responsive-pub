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

package dev.responsive.examples.e2etest;

import dev.responsive.kafka.api.config.StorageBackend;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.admin.Admin;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.RegisterExtension;

public class TestApplicationIntegrationTest {
  private static final String INPUT_TOPIC = "input";
  private static final String OUTPUT_TOPIC = "output";

  @RegisterExtension
  static ResponsiveE2EApplicationTestExtension
      EXTENSION = new ResponsiveE2EApplicationTestExtension(StorageBackend.IN_MEMORY);

  private Admin admin;
  private E2ETestApplication application;
  private E2ETestDriver driver;

  @BeforeEach
  public void before(
      final TestInfo info,
      final Admin admin,
      @ResponsiveConfigParam final Map<String, Object> properties
  ) {
    this.admin = admin;
    this.application = new E2ETestApplication(
        properties,
        "test",
        INPUT_TOPIC,
        OUTPUT_TOPIC,
        4,
        10
    );
    this.driver = new E2ETestDriver(
        properties,
        100,
        INPUT_TOPIC,
        OUTPUT_TOPIC,
        4,
        10000L,
        100,
        Duration.ofMinutes(5),
        Duration.ofMinutes(5),
        "test"
    );
  }

  @Test
  public void shouldRunTestApplication() {
    application.start();
    driver.start();
    application.stop();
    application.await();
  }

  @AfterEach
  public void after() {
    admin.deleteTopics(List.of(INPUT_TOPIC, OUTPUT_TOPIC));
  }
}
