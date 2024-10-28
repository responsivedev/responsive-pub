/*
 * Copyright 2023 Responsive Computing, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dev.responsive.kafka.internal.db.mongo;

import com.mongodb.client.MongoClient;
import com.mongodb.client.model.WriteModel;
import dev.responsive.kafka.api.stores.ResponsiveKeyValueParams;
import dev.responsive.kafka.internal.db.RemoteKVTable;
import dev.responsive.kafka.internal.db.RemoteSessionTable;
import dev.responsive.kafka.internal.db.RemoteWindowedTable;
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
  private final WindowedTableCache<MongoWindowedTable> windowTableCache;
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
        (spec, partitioner) -> new MongoWindowedTable(
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

  public RemoteWindowedTable<WriteModel<WindowDoc>> windowedTable(
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
