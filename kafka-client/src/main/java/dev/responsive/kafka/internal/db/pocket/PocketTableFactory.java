package dev.responsive.kafka.internal.db.pocket;

import dev.responsive.kafka.internal.db.RemoteKVTable;
import dev.responsive.kafka.internal.db.pocket.client.PocketClient;
import dev.responsive.kafka.internal.db.pocket.client.WalEntry;
import dev.responsive.kafka.internal.db.pocket.client.grpc.GrpcPocketClient;
import java.util.List;

public class PocketTableFactory {

  public RemoteKVTable<WalEntry> kvTable(final String name) {
    // todo: make me a config somehow
    final PocketClient pocketClient = GrpcPocketClient.connect("0.0.0.0:5050");
    final PssPartitioner pssPartitioner = PssRangePartitioner.create(
        List.of("000", "001", "010", "011", "100", "101", "110", "111")
    );
    return new PocketKVTable(
        name,
        pocketClient,
        pssPartitioner
    );
  }
}
