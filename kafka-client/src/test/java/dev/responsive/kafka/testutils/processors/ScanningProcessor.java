/*
 * Copyright 2024 Responsive Computing, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package dev.responsive.kafka.testutils.processors;

import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

public class ScanningProcessor implements FixedKeyProcessor<String, String, String> {

  private final String storeName;
  private final Function<KeyValueStore<String, String>, KeyValueIterator<String, String>> storeQuery;

  private KeyValueStore<String, String> kvStore;
  private FixedKeyProcessorContext<String, String> context;

  public ScanningProcessor(
      final String storeName,
      final Function<KeyValueStore<String, String>, KeyValueIterator<String, String>> storeQuery
  ) {
    this.storeName = storeName;
    this.storeQuery = storeQuery;
  }

  @Override
  public void init(final FixedKeyProcessorContext<String, String> context) {
    FixedKeyProcessor.super.init(context);
    this.kvStore = context.getStateStore(storeName);
    this.context = context;
  }

  @Override
  public void process(final FixedKeyRecord<String, String> record) {
    final StringBuilder builder = new StringBuilder();

    try (final KeyValueIterator<String, String> iterator = storeQuery.apply(kvStore)) {

      while (iterator.hasNext()) {
        builder.append(iterator.next().value);
      }
      builder.append(record.value());

    }

    kvStore.put(record.key(), record.value());
    context.forward(record.withValue(builder.toString()));
  }

}
