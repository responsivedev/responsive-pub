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

import dev.responsive.kafka.store.ResponsiveWindowStore;
import dev.responsive.utils.TableName;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowStore;

public class ResponsiveWindowedStoreSupplier implements WindowBytesStoreSupplier {

  private final TableName name;
  private final long retentionPeriod;
  private final long windowSize;
  private final boolean retainDuplicates;

  public ResponsiveWindowedStoreSupplier(
      final String name,
      final long retentionPeriod,
      final long windowSize,
      final boolean retainDuplicates
  ) {
    this.name = new TableName(name);
    this.retentionPeriod = retentionPeriod;
    this.windowSize = windowSize;
    this.retainDuplicates = retainDuplicates;
  }

  @Override
  public String name() {
    return name.kafkaName();
  }

  @Override
  public WindowStore<Bytes, byte[]> get() {
    return new ResponsiveWindowStore(
        name,
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