package dev.responsive.kafka.internal.snapshot.cli;

import de.vandermeer.asciitable.AsciiTable;
import de.vandermeer.asciithemes.a7.A7_Grids;
import dev.responsive.kafka.internal.snapshot.Snapshot;
import dev.responsive.kafka.internal.snapshot.SnapshotApi;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Callable;
import picocli.CommandLine;

@CommandLine.Command(name = "list-snapshots", description = "List all snapshots")
class ListSnapshots implements Callable<Integer> {

  @CommandLine.ParentCommand
  private Main parent;

  public Integer call() {
    final SnapshotApi api = parent.ctx().api();

    final List<Snapshot> snapshots = api.getSnapshots();
    snapshots.sort(Comparator.comparingLong(Snapshot::generation).reversed());

    final AsciiTable table = new AsciiTable();
    table.addRule();
    table.addRow("GENERATION", "CREATED_AT", "STATUS");
    table.addRule();
    for (final Snapshot snapshot : snapshots) {
      table.addRow(snapshot.generation(), snapshot.createdAt(), snapshot.state().name());
      table.addRule();
    }
    table.getContext().setGrid(A7_Grids.minusBarPlusEquals());
    System.out.println(table.render());
    return 0;
  }
}