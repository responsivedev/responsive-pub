package dev.responsive.examples.e2etest;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
  private static final Logger LOG = LoggerFactory.getLogger(Main.class);

  public static void main(String[] args) throws IOException, InterruptedException {
    final Map<String, Object> rawCfg;
    try (InputStream input = new FileInputStream(args[0])) {
      final Properties properties = new Properties();
      properties.load(input);
      rawCfg = properties.keySet().stream()
          .collect(Collectors.toMap(Object::toString, properties::get));
    }
    if (Params.MODE.equals("DRIVER")) {
      LOG.info("starting driver");
      final E2ETestDriver driver = new E2ETestDriver(
          rawCfg,
          Params.NUM_KEYS,
          Params.INPUT_TOPIC,
          Params.OUTPUT_TOPIC,
          Params.PARTITIONS,
          Long.MAX_VALUE,
          Params.MAX_OUTSTANDING
      );
      Runtime.getRuntime().addShutdownHook(new Thread(driver::notifyStop));
      driver.start();
    } else {
      LOG.info("starting application");
      final E2ETestApplication application = new E2ETestApplication(
          rawCfg,
          Params.NAME,
          Params.INPUT_TOPIC,
          Params.OUTPUT_TOPIC,
          Params.PARTITIONS
      );
      Runtime.getRuntime().addShutdownHook(new Thread(application::stop));
      application.start();
      application.await();
    }
  }
}