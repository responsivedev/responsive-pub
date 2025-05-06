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

import dev.responsive.kafka.api.config.ResponsiveConfig;
import dev.responsive.kafka.internal.snapshot.LocalSnapshotApi;
import dev.responsive.kafka.internal.snapshot.SnapshotApi;
import dev.responsive.kafka.internal.snapshot.SnapshotSupport;
import dev.responsive.kafka.internal.snapshot.topic.TopicSnapshotStore;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.kafka.streams.StreamsConfig;
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

    rawCfg.put(ResponsiveConfig.SNAPSHOTS_LOCAL_STORE_TOPIC_REPLICATION_FACTOR, (short) 3);
    rawCfg.put(ResponsiveConfig.SNAPSHOTS_CONFIG, SnapshotSupport.LOCAL.name());

    if (Constants.MODE.equals(Constants.GENERATOR)) {
      startGen(rawCfg);
    } else {
      startValidator(rawCfg);
    }

    rawCfg.put(
        StreamsConfig.APPLICATION_ID_CONFIG,
        OrderSummarizer.NAME + "-true" // -true because the driver adds that
    );
    final SnapshotApi snapshotApi = new LocalSnapshotApi(
        TopicSnapshotStore.create(rawCfg)
    );

    final var exec = Executors.newSingleThreadScheduledExecutor();
    exec.scheduleAtFixedRate(
        () -> {
          try {
            LOG.info("Taking snapshot");
            final var snap = snapshotApi.createSnapshot();
            LOG.info("Took snapshot {}", snap);
          } catch (final RuntimeException e) {
            LOG.warn("Could not create snapshot.", e);
          }
        }, 2, 900, TimeUnit.MINUTES);

    Runtime.getRuntime().addShutdownHook(new Thread(exec::shutdown));
  }

  private static void startValidator(final Map<String, Object> rawCfg) {
    final var streamService = new OrderSummarizer(rawCfg);
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
    final OrderDriver driver = new OrderDriver(rawCfg, 100_000_000);
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      LOG.info("stopping order generator...");
      driver.stopAsync().awaitTerminated();
      LOG.info("stopped order generator");
    }));

    driver.startAsync().awaitRunning();
    LOG.info("started order generator");
  }
}