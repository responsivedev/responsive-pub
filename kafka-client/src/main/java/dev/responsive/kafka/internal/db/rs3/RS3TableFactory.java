package dev.responsive.kafka.internal.db.rs3;

import dev.responsive.kafka.internal.db.RemoteKVTable;
import dev.responsive.kafka.internal.db.rs3.client.WalEntry;
import dev.responsive.kafka.internal.db.rs3.client.grpc.GrpcRS3Client;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

public class RS3TableFactory {
  private final String rs3Host;
  private final int rs3Port;

  public RS3TableFactory(
      final String rs3Host,
      final int rs3Port
  ) {
    this.rs3Host = Objects.requireNonNull(rs3Host);
    this.rs3Port = rs3Port;
  }

  public RemoteKVTable<WalEntry> kvTable(final String name) {
    final UUID storeId = new UUID(0, 0);
    final PssPartitioner pssPartitioner = PssRangePartitioner.create(
        List.of("000", "001", "010", "011", "100", "101", "110", "111")
    );
    final var pocketClient = GrpcRS3Client.connect(
        String.format("%s:%d", rs3Host, rs3Port)
    );
    return new RS3KVTable(
        name,
        storeId,
        pocketClient,
        pssPartitioner
    );
  }

  public void close() {
  }
}
