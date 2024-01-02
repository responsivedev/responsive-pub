/*
 *
 *  * Copyright 2023 Responsive Computing, Inc.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package dev.responsive.kafka.internal.db.mongo;

import static com.mongodb.MongoClientSettings.getDefaultCodecRegistry;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.WriteModel;
import dev.responsive.kafka.internal.db.RemoteWindowedTable;
import dev.responsive.kafka.internal.db.WriterFactory;
import dev.responsive.kafka.internal.db.partitioning.SegmentPartitioner;
import dev.responsive.kafka.internal.utils.WindowedKey;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.bson.Document;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;

public class MongoWindowedTable implements RemoteWindowedTable<WriteModel<Document>> {

  private final String name;
  private final SegmentPartitioner partitioner;

  public MongoWindowedTable(
      final MongoClient client,
      final String name,
      final SegmentPartitioner partitioner
  ) {
    this.name = name;
    this.partitioner = partitioner;

    CodecProvider pojoCodecProvider = PojoCodecProvider.builder().automatic(true).build();
    CodecRegistry pojoCodecRegistry = fromRegistries(
        getDefaultCodecRegistry(),
        fromProviders(pojoCodecProvider)
    );

    final MongoDatabase database = client.getDatabase(name).withCodecRegistry(pojoCodecRegistry);

    // NOTE: the class implementation is split out into a followup PR
  }

  @Override
  public String name() {
    return null;
  }

  @Override
  public WriterFactory<WindowedKey, ?> init(final int kafkaPartition) {
    return null;
  }

  @Override
  public WriteModel<Document> insert(final int kafkaPartition, final WindowedKey key, final byte[] value, final long epochMillis) {
    return null;
  }

  @Override
  public WriteModel<Document> delete(final int kafkaPartition, final WindowedKey key) {
    return null;
  }

  @Override
  public long fetchOffset(final int kafkaPartition) {
    return 0;
  }

  @Override
  public WriteModel<Document> setOffset(final int kafkaPartition, final long offset) {
    return null;
  }

  @Override
  public byte[] fetch(final int partition, final Bytes key, final long windowStart) {
    return new byte[0];
  }

  @Override
  public KeyValueIterator<WindowedKey, byte[]> fetch(final int partition, final Bytes key, final long timeFrom, final long timeTo) {
    return null;
  }

  @Override
  public KeyValueIterator<WindowedKey, byte[]> backFetch(final int partition, final Bytes key, final long timeFrom, final long timeTo) {
    return null;
  }

  @Override
  public KeyValueIterator<WindowedKey, byte[]> fetchRange(final int partition, final Bytes fromKey, final Bytes toKey, final long timeFrom, final long timeTo) {
    return null;
  }

  @Override
  public KeyValueIterator<WindowedKey, byte[]> backFetchRange(final int partition, final Bytes fromKey, final Bytes toKey, final long timeFrom, final long timeTo) {
    return null;
  }

  @Override
  public KeyValueIterator<WindowedKey, byte[]> fetchAll(final int partition, final long timeFrom, final long timeTo) {
    return null;
  }

  @Override
  public KeyValueIterator<WindowedKey, byte[]> backFetchAll(final int partition, final long timeFrom, final long timeTo) {
    return null;
  }
}

