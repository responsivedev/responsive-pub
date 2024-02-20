package dev.responsive.kafka.bootstrap.main;


import dev.responsive.kafka.api.ResponsiveKafkaStreams;
import dev.responsive.kafka.api.stores.ResponsiveKeyValueParams;
import dev.responsive.kafka.bootstrap.ChangelogMigrationTool;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BootstrapMain {
  private static final Logger LOG = LoggerFactory.getLogger(BootstrapMain.class);

  public static void main(final String[] args) {
    final CommandLineParser parser = new DefaultParser();
    final HelpFormatter formatter = new HelpFormatter();

    final CommandLine cmd;
    try {
      cmd = parser.parse(BootstrapOptions.OPTIONS, args);
    } catch (final ParseException e) {
      formatter.printHelp("BoostrapMain", BootstrapOptions.OPTIONS);
      System.exit(1);
      return;
    }

    final Properties properties = new Properties();
    try (final InputStream in
             = new FileInputStream(cmd.getOptionValue(BootstrapOptions.PROPERTIES_FILE))) {
      properties.load(in);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }

    final String name = cmd.getOptionValue(BootstrapOptions.NAME);
    ResponsiveKeyValueParams params = ResponsiveKeyValueParams.fact(name);

    final Duration ttl;
    if (cmd.hasOption(BootstrapOptions.TTL)) {
      ttl = Duration.ofSeconds(
          Long.parseLong(cmd.getOptionValue(BootstrapOptions.TTL))
      );
      params = params.withTimeToLive(ttl);
    } else {
      ttl = null;
    }

    final String changelog = cmd.getOptionValue(BootstrapOptions.CHANGELOG_TOPIC);

    final ChangelogMigrationTool tool = new ChangelogMigrationTool(properties, params, changelog);

    final ResponsiveKafkaStreams app = tool.buildStreams();
    final CountDownLatch closed = new CountDownLatch(1);
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      app.close(Duration.ofMinutes(5));
      closed.countDown();
    }));
    LOG.info("starting bootstrap application for fact store: store({}) ttl({}) changelog({})",
        name,
        ttl == null ? "null" : ttl,
        changelog);
    app.start();
    try {
      LOG.info("blocking until JVM is shut down");
      closed.await();
    } catch (final InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private static class BootstrapOptions {
    public static final Option PROPERTIES_FILE = Option.builder("propertiesFile")
        .required(true)
        .hasArg(true)
        .numberOfArgs(1)
        .desc("path to properties file for bootstrap application")
        .build();

    public static final Option NAME = Option.builder("name")
        .required(true)
        .hasArg(true)
        .numberOfArgs(1)
        .desc("The name of the state store")
        .build();

    public static final Option CHANGELOG_TOPIC = Option.builder("changelogTopic")
        .required(true)
        .hasArg(true)
        .numberOfArgs(1)
        .desc("The name of the store's changelog topic")
        .build();

    public static final Option TTL = Option.builder("ttlSeconds")
        .required(false)
        .hasArg(true)
        .numberOfArgs(1)
        .desc("The TTL for the store in seconds")
        .build();

    public static final Options OPTIONS = new org.apache.commons.cli.Options()
        .addOption(PROPERTIES_FILE)
        .addOption(NAME)
        .addOption(CHANGELOG_TOPIC)
        .addOption(TTL);
  }
}