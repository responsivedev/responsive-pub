/*
 * Copyright 2025 Responsive Computing, Inc.
 *
 * This source code is licensed under the Responsive Business Source License Agreement v1.0
 * available at:
 *
 * https://www.responsive.dev/legal/responsive-bsl-10
 *
 * This software requires a valid Commercial License Key for production use. Trial and commercial
 * licenses can be obtained at https://www.responsive.dev
 */

package dev.responsive.examples.rs3.demo;

import dev.responsive.examples.regression.OrderAndCustomerDriver;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

  private static final Logger LOG = LoggerFactory.getLogger(Main.class);

  public static void main(String[] args) throws Exception {
    final Map<String, Object> rawCfg;

    try (InputStream input = new FileInputStream(args[0])) {
      final Properties properties = new Properties();
      properties.load(input);
      rawCfg = properties.keySet().stream()
          .collect(Collectors.toMap(Object::toString, properties::get));
    }

    startGen(rawCfg);
    startValidator(rawCfg);
  }

  private static void startValidator(final Map<String, Object> rawCfg) {
    final var streamService = new OrderLimitValidator(rawCfg, true);
    LOG.info("starting order validator.");
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      LOG.info("stopping order validator");
      streamService.stopAsync().awaitTerminated();
      LOG.info("stopped order validator");
    }));
    streamService.startAsync().awaitRunning();
    LOG.info("started order validator.");
  }

  private static void startGen(final Map<String, Object> rawCfg) {
    LOG.info("starting the order generator");
    final OrderAndCustomerDriver driver = new OrderAndCustomerDriver(rawCfg, 1000, 100_000_000);
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      LOG.info("stopping order generator...");
      driver.stopAsync().awaitTerminated();
      LOG.info("stopped order generator");
    }));

    driver.startAsync().awaitRunning();
    LOG.info("started order generator");
  }


}
