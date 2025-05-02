package dev.responsive.kafka.internal.snapshot.cli;

import dev.responsive.kafka.internal.snapshot.Snapshot;
import dev.responsive.kafka.internal.snapshot.topic.SnapshotStoreSerdes;
import java.util.concurrent.Callable;
import picocli.CommandLine;

@CommandLine.Command(name = "create-snapshot", description = "Create a new snapshot")
public class CreateSnapshot implements Callable<Integer> {
  @CommandLine.ParentCommand
  private Main parent;

  @Override
  public Integer call() throws Exception {
    final SnapshotContext ctx = parent.ctx();
    final Snapshot snapshot = ctx.api().createSnapshot();
    final String snapshotDescription = SnapshotStoreSerdes.MAPPER
        .writerWithDefaultPrettyPrinter()
        .writeValueAsString(snapshot);
    System.out.println(snapshotDescription);
    return 0;
  }
}
