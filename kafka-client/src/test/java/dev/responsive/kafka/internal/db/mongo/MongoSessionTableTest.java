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

import static dev.responsive.kafka.api.config.ResponsiveConfig.MONGO_CONNECTION_STRING_CONFIG;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

import com.mongodb.client.MongoClient;
import com.mongodb.client.model.Filters;
import dev.responsive.kafka.api.config.StorageBackend;
import dev.responsive.kafka.internal.db.partitioning.SessionSegmentPartitioner;
import dev.responsive.kafka.internal.utils.SessionKey;
import dev.responsive.kafka.internal.utils.SessionUtil;
import dev.responsive.kafka.testutils.ResponsiveConfigParam;
import dev.responsive.kafka.testutils.ResponsiveExtension;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.RegisterExtension;

class MongoSessionTableTest {

  @RegisterExtension
  public static final ResponsiveExtension EXT = new ResponsiveExtension(StorageBackend.MONGO_DB);
  private static final CollectionCreationOptions UNSHARDED = new CollectionCreationOptions(
      false,
      0
  );
  private static final byte[] DEFAULT_VALUE = new byte[] {1};

  private String name;
  private MongoClient client;

  @BeforeEach
  public void before(
      final TestInfo info,
      @ResponsiveConfigParam final Map<String, Object> props
  ) {
    name = info.getDisplayName().replace("()", "");

    final String mongoConnection = (String) props.get(MONGO_CONNECTION_STRING_CONFIG);
    client = SessionUtil.connect(mongoConnection, "", null);
  }

  /*
   * - Insert a session key with start/end [0, 100]
   * - Successfully retrieve the session when querying the remote table with start/end [0, 100]
   * - Correctly fail to retrieve the session when querying the remote table with
   *   start/end [0, 200]
   * - Correctly fail to retrieve the session when querying the remote table with the wrong key
   * */
  @Test
  public void shouldSucceedSimpleSetGet() {
    // Given:
    final SessionSegmentPartitioner partitioner = new SessionSegmentPartitioner(10_000L, 1_000L);
    final var segment = partitioner.segmenter().activeSegments(0, 100).get(0);

    final MongoSessionTable table = new MongoSessionTable(client, name, partitioner, UNSHARDED);
    final var flushManager = table.init(0);
    flushManager.updateOffsetAndStreamTime(0, 100);
    flushManager.createSegment(segment);

    // When:
    final var byteKey = Bytes.wrap("key".getBytes());
    var writer = flushManager.createWriter(segment, 0);
    writer.insert(
        new SessionKey(byteKey, 0, 100),
        DEFAULT_VALUE,
        table.localEpoch(0)
    );
    writer.flush();

    // Then:
    var value = table.fetch(0, byteKey, 0, 100);
    assertThat(value, Matchers.equalTo(DEFAULT_VALUE));
    value = table.fetch(0, byteKey, 0, 200);
    assertThat(value, Matchers.nullValue());
    value = table.fetch(0, Bytes.wrap("other".getBytes()), 0, 100);
    assertThat(value, Matchers.nullValue());
  }

  /*
   * - Insert a session key with start/end [0, 100]
   * - Delete the session key that was just inserted.
   * - Correctly fail to retrieve the session when querying the remote table with start/end [0, 100]
   * - Correctly fail to retrieve the session when querying the remote table with start/end [0, 200]
   * - Correctly fail to retrieve the session when querying the remote table with the wrong key
   * - Correctly fail to retrieve the session when querying the remote table with a range query
   * */
  @Test
  public void shouldDeleteProperly() {
    // Given:
    final SessionSegmentPartitioner partitioner = new SessionSegmentPartitioner(10_000L, 1_000L);
    final var segment = partitioner.segmenter().activeSegments(0, 100).get(0);

    final MongoSessionTable table = new MongoSessionTable(client, name, partitioner, UNSHARDED);
    final var flushManager = table.init(0);
    flushManager.updateOffsetAndStreamTime(0, 100);
    flushManager.createSegment(segment);

    // When:
    final var byteKey = Bytes.wrap("key".getBytes());
    final var sessionKey = new SessionKey(byteKey, 0, 100);
    var writer = flushManager.createWriter(segment, 0);
    writer.insert(
        sessionKey,
        DEFAULT_VALUE,
        table.localEpoch(0)
    );
    writer.flush();

    writer.delete(sessionKey);
    writer.flush();

    // Then:
    var value = table.fetch(0, byteKey, 0, 100);
    assertThat(value, Matchers.nullValue());
    value = table.fetch(0, byteKey, 0, 200);
    assertThat(value, Matchers.nullValue());
    value = table.fetch(0, Bytes.wrap("other".getBytes()), 0, 100);
    assertThat(value, Matchers.nullValue());
    var values = table.fetchAll(0, byteKey, 0, 200);
    assertThat("no value should be returned from fetchAll",
        values.hasNext(), Matchers.equalTo(false)
    );
  }

  /*
   * - Insert a session key A with start/end [0, 100]
   * - Insert a session key B with start/end [0, 150]
   * - Correctly retrieve session key A with a range query from [50, 150]
   * - Correctly retrieve session key B with a range query from [50, 150]
   * - Correctly fail to retrieve session key A with a range query from [150, 250]
   * */
  @Test
  public void shouldSucceedFetchAll() {
    // Given:
    final SessionSegmentPartitioner partitioner = new SessionSegmentPartitioner(10_000L, 1_000L);
    final var segment = partitioner.segmenter().activeSegments(0, 100).get(0);

    final MongoSessionTable table = new MongoSessionTable(client, name, partitioner, UNSHARDED);
    final var flushManager = table.init(0);
    flushManager.updateOffsetAndStreamTime(0, 100);
    flushManager.createSegment(segment);

    // When:
    final var byteKey = Bytes.wrap("key".getBytes());
    final var sessionKey1 = new SessionKey(byteKey, 0, 100);
    final var sessionKey2 = new SessionKey("other".getBytes(), 0, 150);
    final var sessionKey3 = new SessionKey(byteKey, 200, 300);
    var writer = flushManager.createWriter(segment, 0);
    writer.insert(
        sessionKey1,
        DEFAULT_VALUE,
        table.localEpoch(0)
    );
    writer.insert(
        sessionKey2,
        DEFAULT_VALUE,
        table.localEpoch(0)
    );
    writer.insert(
        sessionKey3,
        DEFAULT_VALUE,
        table.localEpoch(0)
    );
    writer.flush();

    // Then:
    var it = table.fetchAll(0, byteKey, 100, 200);
    var kvs = new ArrayList<KeyValue<SessionKey, byte[]>>();
    it.forEachRemaining(kvs::add);
    assertThat(kvs, Matchers.hasSize(1));
    assertThat("key matches", kvs.get(0).key.equals(sessionKey1));
    assertThat("value matches", Arrays.equals(kvs.get(0).value, DEFAULT_VALUE));

    it = table.fetchAll(0, Bytes.wrap("other".getBytes()), 100, 200);
    kvs = new ArrayList<KeyValue<SessionKey, byte[]>>();
    it.forEachRemaining(kvs::add);
    assertThat(kvs, Matchers.hasSize(1));
    assertThat("key matches", kvs.get(0).key.equals(sessionKey2));
    assertThat("value matches", Arrays.equals(kvs.get(0).value, DEFAULT_VALUE));

    it = table.fetchAll(0, byteKey, 150, 250);
    kvs = new ArrayList<KeyValue<SessionKey, byte[]>>();
    it.forEachRemaining(kvs::add);
    assertThat(kvs, Matchers.hasSize(0));

    it = table.fetchAll(0, byteKey, 100, 400);
    kvs = new ArrayList<KeyValue<SessionKey, byte[]>>();
    it.forEachRemaining(kvs::add);
    assertThat(kvs, Matchers.hasSize(2));
    assertThat("key matches", kvs.get(0).key.equals(sessionKey1));
    assertThat("value matches", Arrays.equals(kvs.get(0).value, DEFAULT_VALUE));
    assertThat("key matches", kvs.get(1).key.equals(sessionKey3));
    assertThat("value matches", Arrays.equals(kvs.get(1).value, DEFAULT_VALUE));
  }

  @Test
  public void shouldEncodeKeysUsingBase64() {
    final SessionSegmentPartitioner partitioner = new SessionSegmentPartitioner(10_000L, 1_000L);
    final var segment = partitioner.segmenter().activeSegments(0, 100).get(0);
    final MongoSessionTable table = new MongoSessionTable(client, name, partitioner, UNSHARDED);
    final var flushManager = table.init(0);
    flushManager.updateOffsetAndStreamTime(0, 100);
    flushManager.createSegment(segment);
    final var byteKey = Bytes.wrap("key".getBytes());
    final var sessionKey = new SessionKey(byteKey, 0, 100);
    var writer = flushManager.createWriter(segment, 0);
    writer.insert(
        sessionKey,
        DEFAULT_VALUE,
        table.localEpoch(0)
    );
    writer.flush();

    // when:
    final var collection = table.collectionForSegmentPartition(0, segment);
    final var result = collection.find(
        Filters.eq(
            SessionDoc.ID,
            SessionDoc.compositeKey(
                new StringKeyCodec().encode(byteKey),
                0,
                100
            ))
    ).first();

    assertThat(result, not(nullValue()));
  }
}