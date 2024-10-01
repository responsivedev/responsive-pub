/*
 * Copyright 2024 Responsive Computing, Inc.
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

import static dev.responsive.kafka.api.config.ResponsiveConfig.MONGO_ENDPOINT_CONFIG;
import static org.hamcrest.MatcherAssert.assertThat;

import com.mongodb.client.MongoClient;
import dev.responsive.kafka.api.config.StorageBackend;
import dev.responsive.kafka.internal.db.partitioning.WindowSegmentPartitioner;
import dev.responsive.kafka.internal.utils.SessionUtil;
import dev.responsive.kafka.internal.utils.WindowedKey;
import dev.responsive.kafka.testutils.ResponsiveConfigParam;
import dev.responsive.kafka.testutils.ResponsiveExtension;
import java.util.ArrayList;
import java.util.Map;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.RegisterExtension;

class MongoWindowTableTest {

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

    final String mongoConnection = (String) props.get(MONGO_ENDPOINT_CONFIG);
    client = SessionUtil.connect(mongoConnection, null, null, "", null);
  }

  @Test
  public void shouldSucceedSimpleSetGet() {
    // Given:
    final WindowSegmentPartitioner partitioner =
        new WindowSegmentPartitioner(10_000L, 1_000L, false);
    final var segment = partitioner.segmenter().activeSegments(0, 100).get(0);

    final MongoWindowedTable table =
        new MongoWindowedTable(client, name, partitioner, false, UNSHARDED);

    final var flushManager = table.init(0);
    flushManager.updateOffsetAndStreamTime(0, 100);
    flushManager.createSegment(segment);

    // When:
    final var byteKey = Bytes.wrap("key".getBytes());
    var writer = flushManager.createWriter(segment);
    writer.insert(
        new WindowedKey(byteKey, 0),
        DEFAULT_VALUE,
        table.localEpoch(0)
    );
    writer.flush();

    // Then:
    var value = table.fetch(0, byteKey, 0);
    assertThat(value, Matchers.equalTo(DEFAULT_VALUE));
    value = table.fetch(0, byteKey, 100);
    assertThat(value, Matchers.nullValue());
    value = table.fetch(0, Bytes.wrap("other".getBytes()), 0);
    assertThat(value, Matchers.nullValue());
  }

  @Test
  public void shouldSucceedRangeSetGet() {
    // Given:
    final WindowSegmentPartitioner partitioner =
        new WindowSegmentPartitioner(10_000L, 1_000L, false);
    final var segment = partitioner.segmenter().activeSegments(0, 100).get(0);

    final MongoWindowedTable table =
        new MongoWindowedTable(client, name, partitioner, false, UNSHARDED);
    final var flushManager = table.init(0);
    flushManager.updateOffsetAndStreamTime(0, 6_000);
    flushManager.createSegment(segment);

    // When:
    final var byteKey = Bytes.wrap("key".getBytes());
    final var windowedKey1 = new WindowedKey(byteKey, 500);
    final var windowedKey2 = new WindowedKey(byteKey, 5_000);
    var writer = flushManager.createWriter(segment);
    writer.insert(
        windowedKey1,
        DEFAULT_VALUE,
        table.localEpoch(0)
    );
    writer.insert(
        windowedKey2,
        DEFAULT_VALUE,
        table.localEpoch(0)
    );
    writer.flush();

    // Then:
    var it = table.fetch(0, byteKey, 100, 6_000);
    var kvs = new ArrayList<KeyValue<WindowedKey, byte[]>>();
    it.forEachRemaining(kvs::add);

    assertThat(kvs, Matchers.hasSize(2));
    assertThat(kvs.get(0).key.key, Matchers.equalTo(windowedKey1.key));
    assertThat(kvs.get(0).key.windowStartMs, Matchers.equalTo(windowedKey1.windowStartMs));
    assertThat(kvs.get(0).value, Matchers.equalTo(DEFAULT_VALUE));

    assertThat(kvs.get(1).key.key, Matchers.equalTo(windowedKey2.key));
    assertThat(kvs.get(1).key.windowStartMs, Matchers.equalTo(windowedKey2.windowStartMs));
    assertThat(kvs.get(1).value, Matchers.equalTo(DEFAULT_VALUE));
  }
}