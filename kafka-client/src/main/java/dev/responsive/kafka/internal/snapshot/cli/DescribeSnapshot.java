package dev.responsive.kafka.internal.snapshot.cli;

import dev.responsive.kafka.internal.snapshot.Snapshot;
import dev.responsive.kafka.internal.snapshot.topic.SnapshotStoreSerdes;
import java.util.Optional;
import java.util.concurrent.Callable;
import picocli.CommandLine;
//
@CommandLine.Command(name = "describe-snapshot", description = "Describe a snapshot")
public class DescribeSnapshot implements Callable<Integer> {
  @CommandLine.ParentCommand
  private Main parent;

  @CommandLine.Option(names = "--generation", description = "Snapshot generation", required = true)
  private Long generation;

  @Override
  public Integer call() throws Exception {
    final SnapshotContext ctx = parent.ctx();
    final Optional<Snapshot> maybeSnapshot = ctx.api().getSnapshots()
        .stream()
        .filter(s -> s.generation() == generation)
        .findFirst();
    if (maybeSnapshot.isEmpty()) {
      System.err.println("No snapshot found for generation " + generation);
      return -1;
    }
    final Snapshot snapshot = maybeSnapshot.get();
    final String snapshotDescription = SnapshotStoreSerdes.MAPPER
        .writerWithDefaultPrettyPrinter()
        .writeValueAsString(snapshot);
    System.out.println(snapshotDescription);
    return 0;
  }
}
