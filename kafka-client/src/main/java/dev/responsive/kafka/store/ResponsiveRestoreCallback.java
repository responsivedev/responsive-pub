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
package dev.responsive.kafka.store;

import java.util.Collection;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.internals.RecordBatchingStateRestoreCallback;

public class ResponsiveRestoreCallback implements RecordBatchingStateRestoreCallback {

  private final Consumer<Collection<ConsumerRecord<byte[], byte[]>>> batchRestorer;
  private final Supplier<Boolean> isActive;

  public ResponsiveRestoreCallback(
      final Consumer<Collection<ConsumerRecord<byte[], byte[]>>> batchRestorer,
      final Supplier<Boolean> isActive
  ) {
    this.batchRestorer = batchRestorer;
    this.isActive = isActive;
  }

  @Override
  public void restoreBatch(final Collection<ConsumerRecord<byte[], byte[]>> records) {
    if (isActive.get()) {
      batchRestorer.accept(records);
    }
  }
}
