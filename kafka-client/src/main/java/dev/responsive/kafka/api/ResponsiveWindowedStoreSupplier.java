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

package dev.responsive.kafka.api;

import dev.responsive.db.CassandraClient;
import dev.responsive.kafka.store.ResponsiveWindowStore;
import dev.responsive.utils.RemoteMonitor;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowStore;

public class ResponsiveWindowedStoreSupplier implements WindowBytesStoreSupplier {

  private final String name;
  private final long retentionPeriod;
  private final long windowSize;
  private final boolean retainDuplicates;
  private final CassandraClient client;
  private final RemoteMonitor awaitTable;
  private final Admin admin;

  public ResponsiveWindowedStoreSupplier(
      final CassandraClient client,
      final String name,
      final ScheduledExecutorService executorService,
      final Admin admin,
      final long retentionPeriod,
      final long windowSize,
      final boolean retainDuplicates
  ) {
    this.client = client;
    this.name = name;
    this.admin = admin;
    this.retentionPeriod = retentionPeriod;
    this.windowSize = windowSize;
    this.retainDuplicates = retainDuplicates;

    // we maintain the name without quotes because quotes are
    // not valid in Kafka topics, but on the other hand they
    // are necessary to ensure that Cassandra can accept whatever
    // the client tosses at it
    awaitTable = client.awaitTable('"' + name + '"', executorService);
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public WindowStore<Bytes, byte[]> get() {
    return new ResponsiveWindowStore(
        client,
        name,
        awaitTable,
        admin,
        retentionPeriod,
        windowSize,
        retainDuplicates
    );
  }

  // Responsive window store is not *really* segmented, so just
  // say size is 1 ms this is what InMemoryWindowBytesStoreSupplier
  // does in Kafka Streams
  @Override
  public long segmentIntervalMs() {
    return 1;
  }

  @Override
  public long windowSize() {
    return windowSize;
  }

  @Override
  public boolean retainDuplicates() {
    return retainDuplicates;
  }

  @Override
  public long retentionPeriod() {
    return retentionPeriod;
  }

  @Override
  public String metricsScope() {
    return "responsive-window";
  }
}