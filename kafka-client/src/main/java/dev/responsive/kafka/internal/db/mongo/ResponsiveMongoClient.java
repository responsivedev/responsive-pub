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

package dev.responsive.kafka.internal.db.mongo;

import com.mongodb.client.MongoClient;
import com.mongodb.client.model.WriteModel;
import dev.responsive.kafka.internal.db.RemoteKVTable;
import dev.responsive.kafka.internal.db.RemoteSessionTable;
import dev.responsive.kafka.internal.db.RemoteWindowTable;
import dev.responsive.kafka.internal.db.SessionTableCache;
import dev.responsive.kafka.internal.db.TableCache;
import dev.responsive.kafka.internal.db.WindowedTableCache;
import dev.responsive.kafka.internal.db.partitioning.SessionSegmentPartitioner;
import dev.responsive.kafka.internal.db.partitioning.TablePartitioner;
import dev.responsive.kafka.internal.db.partitioning.WindowSegmentPartitioner;
import dev.responsive.kafka.internal.db.spec.DefaultTableSpec;
import dev.responsive.kafka.internal.stores.TtlResolver;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

public class ResponsiveMongoClient {

  private final TableCache<MongoKVTable> kvTableCache;
  private final WindowedTableCache<MongoWindowTable> windowTableCache;
  private final SessionTableCache<MongoSessionTable> sessionTableCache;
  private final MongoClient client;

  public ResponsiveMongoClient(
      final MongoClient client,
      final boolean timestampFirstOrder,
      final CollectionCreationOptions collectionCreationOptions
  ) {
    this.client = client;
    kvTableCache = new TableCache<>(
        spec -> new MongoKVTable(
            client,
            spec.tableName(),
            collectionCreationOptions,
            spec.ttlResolver()
        ));
    windowTableCache = new WindowedTableCache<>(
        (spec, partitioner) -> new MongoWindowTable(
            client,
            spec.tableName(),
            partitioner,
            timestampFirstOrder,
            collectionCreationOptions
        )
    );
    sessionTableCache = new SessionTableCache<>(
        (spec, partitioner) -> new MongoSessionTable(
            client,
            spec.tableName(),
            partitioner,
            collectionCreationOptions
        )
    );
  }

  public RemoteKVTable<WriteModel<KVDoc>> kvTable(
      final String name,
      final Optional<TtlResolver<?, ?>> ttlResolver
  ) throws InterruptedException, TimeoutException {
    return kvTableCache.create(
        new DefaultTableSpec(name, TablePartitioner.defaultPartitioner(), ttlResolver)
    );
  }

  public RemoteWindowTable<WriteModel<WindowDoc>> windowedTable(
      final String name,
      final WindowSegmentPartitioner partitioner
  ) throws InterruptedException, TimeoutException {
    return windowTableCache.create(
        new DefaultTableSpec(name, partitioner, Optional.empty()), partitioner);
  }

  public RemoteSessionTable<WriteModel<SessionDoc>> sessionTable(
      final String name,
      final SessionSegmentPartitioner partitioner
  ) throws InterruptedException, TimeoutException {
    return sessionTableCache.create(
        new DefaultTableSpec(name, partitioner, Optional.empty()), partitioner);
  }

  public void close() {
    client.close();
  }
}
