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

package dev.responsive.kafka.internal.stores;

import static org.apache.kafka.streams.processor.internals.ProcessorContextUtils.asInternalProcessorContext;

import dev.responsive.kafka.internal.db.CassandraClient;
import dev.responsive.kafka.internal.utils.TableName;
import java.util.Collection;
import java.util.function.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.query.Position;

public abstract class AbstractResponsiveStore implements StateStore {

  protected final TableName name;
  protected final Position position; // TODO(IQ): update the position during restoration

  // Initialized during #init
  @SuppressWarnings("rawtypes")
  protected InternalProcessorContext context;
  protected TopicPartition changelog;
  protected CassandraClient client;
  protected ResponsiveStoreRegistry storeRegistry;
  protected ResponsiveStoreRegistration registration;

  private boolean open;

  public AbstractResponsiveStore(final TableName name) {
    this.name = name;
    this.position = Position.emptyPosition();
  }

  @SuppressWarnings("rawtypes")
  protected void init(
      final StateStoreContext storeContext,
      final CassandraClient client,
      final ResponsiveStoreRegistry storeRegistry,
      final TopicPartition changelog
  ) {
    this.context = asInternalProcessorContext(storeContext);
    this.client = client;
    this.storeRegistry = storeRegistry;
    this.changelog = changelog;
  }

  protected void open(
      final StateStore root,
      final Consumer<Collection<ConsumerRecord<byte[], byte[]>>> restoreBatch
  ) {
    context.register(root, new ResponsiveRestoreCallback(restoreBatch));
    open = true;
  }

  @Override
  public String name() {
    return name.kafkaName();
  }

  @Override
  public boolean isOpen() {
    return open;
  }

  @Override
  public boolean persistent() {
    // Kafka Streams uses this to determine whether it
    // needs to create and lock state directories. since
    // the Responsive Client doesn't require flushing state
    // to disk, we return false even though the store is
    // persistent in a remote store
    return false;
  }

  @Override
  public Position getPosition() {
    return position;
  }

  @Override
  public void flush() {
    // Kafka Streams uses this to determine whether it
    // needs to create and lock state directories. since
    // the Responsive Client doesn't require flushing state
    // to disk, we return false even though the store is
    // persistent in a remote store
  }
}
