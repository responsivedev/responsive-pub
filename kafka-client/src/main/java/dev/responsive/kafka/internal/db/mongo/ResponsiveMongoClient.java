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
import dev.responsive.kafka.internal.db.RemoteKVTable;
import dev.responsive.kafka.internal.db.RemoteWindowedTable;
import dev.responsive.kafka.internal.db.TableCache;
import dev.responsive.kafka.internal.db.partitioning.SegmentPartitioner;
import dev.responsive.kafka.internal.db.partitioning.TablePartitioner;
import dev.responsive.kafka.internal.db.spec.BaseTableSpec;
import java.util.concurrent.TimeoutException;

public class ResponsiveMongoClient {

  private final TableCache<MongoKVTable> kvTableCache;
  private final TableCache<MongoWindowedTable> windowTableCache;
  private final MongoClient client;

  public ResponsiveMongoClient(final MongoClient client, final boolean timestampFirstOrder) {
    this.client = client;
    kvTableCache = new TableCache<>(spec -> new MongoKVTable(client, spec.tableName()));
    windowTableCache = new TableCache<>(
        spec -> new MongoWindowedTable(
            client,
            spec.tableName(),
            (SegmentPartitioner) spec.partitioner(),
            timestampFirstOrder,
            true
        )
    );
  }

  public RemoteKVTable<WriteModel<KVDoc>> kvTable(final String name)
      throws InterruptedException, TimeoutException {
    return kvTableCache.create(new BaseTableSpec(name, TablePartitioner.defaultPartitioner()));
  }

  public RemoteWindowedTable<WriteModel<WindowDoc>> windowedTable(
      final String name,
      final SegmentPartitioner partitioner
  ) throws InterruptedException, TimeoutException {
    return windowTableCache.create(new BaseTableSpec(name, partitioner));
  }

  public void close() {
    client.close();
  }
}
