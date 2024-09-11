package dev.responsive.kafka.internal.db.pocket;

import dev.responsive.kafka.internal.db.RemoteKVTable;
import dev.responsive.kafka.internal.db.pocket.client.PocketClient;
import dev.responsive.kafka.internal.db.pocket.client.WalEntry;
import dev.responsive.kafka.internal.db.pocket.client.grpc.GrpcPocketClient;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

public class PocketTableFactory {
  private final String pocketHost;
  private final int pocketPort;

  public PocketTableFactory(
      final String pocketHost,
      final int pocketPort
  ) {
    this.pocketHost = Objects.requireNonNull(pocketHost);
    this.pocketPort = pocketPort;
  }

  public RemoteKVTable<WalEntry> kvTable(final String name) {
    final UUID storeId = new UUID(0, 0);
    final PssPartitioner pssPartitioner = PssRangePartitioner.create(
        List.of("000", "001", "010", "011", "100", "101", "110", "111")
    );
    final var pocketClient = GrpcPocketClient.connect(
        String.format("%s:%d", pocketHost, pocketPort)
    );
    return new PocketKVTable(
        name,
        storeId,
        pocketClient,
        pssPartitioner
    );
  }

  public void close() {
  }
}
