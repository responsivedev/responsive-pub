/*
 * Copyright 2024 Responsive Computing, Inc.
 *
 * This source code is licensed under the Responsive Business Source License Agreement v1.0
 * available at:
 *
 * https://www.responsive.dev/legal/responsive-bsl-10
 *
 * This software requires a valid Commercial License Key for production use. Trial and commercial
 * licenses can be obtained at https://www.responsive.dev
 */

package dev.responsive.kafka.internal.db.rs3;

import dev.responsive.kafka.api.config.ResponsiveConfig;
import dev.responsive.kafka.internal.db.RemoteKVTable;
import dev.responsive.kafka.internal.db.rs3.client.WalEntry;
import dev.responsive.kafka.internal.db.rs3.client.grpc.GrpcRS3Client;
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

  public RemoteKVTable<WalEntry> kvTable(final String name, final ResponsiveConfig config) {
    final UUID storeId = new UUID(0, 0);
    final PssPartitioner pssPartitioner = new PssDirectPartitioner();
    final var rs3Client = GrpcRS3Client.connect(
        String.format("%s:%d", rs3Host, rs3Port)
    );
    return new RS3KVTable(
        name,
        storeId,
        rs3Client,
        pssPartitioner
    );
  }

  public void close() {
  }
}
