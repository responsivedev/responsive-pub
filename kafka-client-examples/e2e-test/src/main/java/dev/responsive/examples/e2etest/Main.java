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

import dev.responsive.examples.common.JsonDeserializer;
import dev.responsive.examples.common.Mode;
import dev.responsive.examples.regression.OrderAndCustomerDriver;
import dev.responsive.examples.regression.RegressionSchema;
import dev.responsive.examples.regression.ResultsComparatorService;
import dev.responsive.examples.regression.tests.AbstractKSExampleService;
import dev.responsive.examples.regression.tests.FullTopologyExample;
import dev.responsive.examples.regression.tests.KeyBatchExample;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
  private static final Logger LOG = LoggerFactory.getLogger(Main.class);

  public static void main(String[] args) throws IOException {
    final Map<String, Object> rawCfg;
    try (InputStream input = new FileInputStream(args[0])) {
      final Properties properties = new Properties();
      properties.load(input);
      rawCfg = properties.keySet().stream()
          .collect(Collectors.toMap(Object::toString, properties::get));
    }
    final var mode = Mode.valueOf(Params.MODE);
    switch (mode) {
      case DRIVER:
        startDriver(rawCfg);
        break;
      case APPLICATION:
        startApplication(rawCfg);
        break;

      // TODO(agavra): put this information into the enum so we don't need a big switch
      case REGRESSION_ST_DRIVER:
        startRegressionDriver(rawCfg, RegressionSchema.EnrichedOrderDeserializer.class);
        break;
      case REGRESSION_ST_JOIN:
        startRegressionTest(new FullTopologyExample(rawCfg, true));
        break;
      case REGRESSION_ST_BASELINE:
        startRegressionTest(new FullTopologyExample(rawCfg, false));
        break;

      case REGRESSION_BATCH_DRIVER:
        startRegressionDriver(rawCfg, RegressionSchema.GroupedOrderDeserializer.class);
        break;
      case REGRESSION_BATCH:
        startRegressionTest(new KeyBatchExample(rawCfg, true));
        break;
      case REGRESSION_BATCH_BASELINE:
        startRegressionTest(new KeyBatchExample(rawCfg, false));
        break;
      default:
        throw new IllegalArgumentException("Unexpected mode: " + mode);
    }
  }

  private static <T extends Comparable<T>> void startRegressionDriver(
      final Map<String, Object> rawCfg,
      final Class<? extends JsonDeserializer<T>> deserializer
  ) {
    LOG.info("starting regression driver");
    final OrderAndCustomerDriver driver = new OrderAndCustomerDriver(rawCfg);
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      LOG.info("stopping regression driver...");
      driver.stopAsync().awaitTerminated();
      LOG.info("stopped regression driver");
    }));

    // start the driver and await running before starting
    // the comparator so we can ensure that all the topics
    // in Kafka have been created
    driver.startAsync().awaitRunning();
    LOG.info("started regression driver");

    LOG.info("starting regression comparator");
    final ResultsComparatorService<?> comparator =
        new ResultsComparatorService<>(rawCfg, deserializer);
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      LOG.info("stopping regression comparator...");
      comparator.stopAsync().awaitTerminated();
      LOG.info("stopped regression comparator");
    }));

    comparator.startAsync().awaitRunning();
    LOG.info("started regression comparator");
  }

  private static void startRegressionTest(final AbstractKSExampleService service) {
    LOG.info("starting regression {}", service.name());
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      LOG.info("stopping regression {}...", service.name());
      service.stopAsync().awaitTerminated();
      LOG.info("stopped regression {}", service.name());
    }));
    service.startAsync().awaitRunning();
    LOG.info("started regression {}.", service.name());
  }

  private static void startApplication(final Map<String, Object> rawCfg) {
    LOG.info("starting application");
    final E2ETestApplication application = new E2ETestApplication(
        rawCfg,
        Params.NAME,
        Params.INPUT_TOPIC,
        Params.OUTPUT_TOPIC,
        Params.PARTITIONS,
        Params.EXCEPTION_INJECT_THRESHOLD
    );
    Runtime.getRuntime().addShutdownHook(new Thread(application::stop));
    application.start();
    application.await();
  }

  private static void startDriver(final Map<String, Object> rawCfg) {
    LOG.info("starting driver");
    final E2ETestDriver driver = new E2ETestDriver(
        rawCfg,
        Params.NUM_KEYS,
        Params.INPUT_TOPIC,
        Params.OUTPUT_TOPIC,
        Params.PARTITIONS,
        Long.MAX_VALUE,
        Params.MAX_OUTSTANDING,
        Duration.ofSeconds(Params.RECEIVE_THRESHOLD),
        Duration.ofSeconds(Params.FAULT_STOP_THRESHOLD),
        Params.NAME
    );
    Runtime.getRuntime().addShutdownHook(new Thread(driver::notifyStop));
    driver.start();
  }
}