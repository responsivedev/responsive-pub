package dev.responsive.kafka.internal.snapshot.cli;

import dev.responsive.kafka.api.config.ResponsiveConfig;
import dev.responsive.kafka.internal.snapshot.LocalSnapshotApi;
import dev.responsive.kafka.internal.snapshot.SnapshotApi;
import dev.responsive.kafka.internal.snapshot.SnapshotStore;
import dev.responsive.kafka.internal.snapshot.SnapshotSupport;
import dev.responsive.kafka.internal.snapshot.topic.TopicSnapshotStore;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.apache.kafka.streams.StreamsConfig;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(
    name = "snapshot-cli",
    mixinStandardHelpOptions = true,
    version = "1.0",
    description = "Responsive Snapshot Management CLI",
    subcommands = { ListSnapshots.class, DescribeSnapshot.class, CreateSnapshot.class }
)
public class Main implements Runnable {

  @Option(names = "--config", description = "Path to config file", required = true)
  private String configPath;

  private SnapshotContext ctx;

  private void init() {
    final Properties rawConfig = new Properties();
    try (final InputStream inputStream = new FileInputStream(configPath)) {
      rawConfig.load(inputStream);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    final ResponsiveConfig config = ResponsiveConfig.responsiveConfig(rawConfig);
    final SnapshotSupport support = SnapshotSupport
        .valueOf(config.getString(ResponsiveConfig.SNAPSHOTS_CONFIG));
    switch (support) {
      case LOCAL: {
        final SnapshotStore store = TopicSnapshotStore.create(config.originals());
        final SnapshotApi api = new LocalSnapshotApi(store);
        ctx = new SnapshotContext(api);
        break;
      }
      case DISABLED: throw new RuntimeException("snapshots are not enabled in config");
    }
  }

  SnapshotContext ctx() {
    return ctx;
  }

  public void run() {
    System.out.println("Please specify a subcommand. Use --help for more info.");
  }

  private void close() {
    if (ctx != null) {
      ctx.close();
    }
  }

  public static void main(String[] args) {
    final Main main = new Main();
    int exitCode = new CommandLine(main)
        .setExecutionStrategy(pr -> {
          main.init();
          return new CommandLine.RunLast().execute(pr);
        })
        .execute(args);
    main.close();
    System.exit(exitCode);
  }
}
