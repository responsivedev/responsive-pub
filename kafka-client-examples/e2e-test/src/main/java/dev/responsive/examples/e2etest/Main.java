package dev.responsive.examples.e2etest;

import dev.responsive.examples.common.Mode;
import dev.responsive.examples.regression.OrderAndCustomerDriver;
import dev.responsive.examples.regression.OrderProcessingExample;
import dev.responsive.examples.regression.ResultsComparatorService;
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
      case REGRESSION_DRIVER:
        startRegressionDriver(rawCfg);
        break;
      case REGRESSION_ST_JOIN:
        startRegressionStJoin(rawCfg, true);
        break;
      case REGRESSION_BASELINE:
        startRegressionStJoin(rawCfg, false);
        break;
      default:
        throw new IllegalArgumentException("Unexpected mode: " + mode);
    }
  }

  private static void startRegressionDriver(final Map<String, Object> rawCfg) {
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
    final ResultsComparatorService comparator = new ResultsComparatorService(rawCfg);
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      LOG.info("stopping regression comparator...");
      comparator.stopAsync().awaitTerminated();
      LOG.info("stopped regression comparator");
    }));

    comparator.startAsync().awaitRunning();
    LOG.info("started regression comparator");
  }

  private static void startRegressionStJoin(
      final Map<String, Object> rawCfg,
      final boolean responsive
  ) {
    LOG.info("starting regression ST-JOIN test");
    final OrderProcessingExample regression = new OrderProcessingExample(rawCfg, responsive);
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      LOG.info("stopping regression ST-JOIN test...");
      regression.stopAsync().awaitTerminated();
      LOG.info("stopped regression ST-JOIN test");
    }));
    regression.startAsync().awaitRunning();
    LOG.info("started regression ST-JOIN test.");
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