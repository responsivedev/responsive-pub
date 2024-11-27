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

import static dev.responsive.kafka.api.config.ResponsiveConfig.MONGO_ENDPOINT_CONFIG;
import static dev.responsive.kafka.testutils.Matchers.sameKeyValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

import com.mongodb.client.MongoClient;
import com.mongodb.client.model.Filters;
import dev.responsive.kafka.api.config.StorageBackend;
import dev.responsive.kafka.internal.db.MongoWindowFlushManager;
import dev.responsive.kafka.internal.db.partitioning.Segmenter;
import dev.responsive.kafka.internal.db.partitioning.WindowSegmentPartitioner;
import dev.responsive.kafka.internal.utils.SessionUtil;
import dev.responsive.kafka.internal.utils.WindowedKey;
import dev.responsive.kafka.testutils.ResponsiveConfigParam;
import dev.responsive.kafka.testutils.ResponsiveExtension;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
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

  private String name;
  private MongoClient client;
  private WindowSegmentPartitioner partitioner;
  private Segmenter.SegmentPartition segment;
  private MongoWindowTable table;
  private MongoWindowFlushManager flushManager;

  @BeforeEach
  public void before(
      final TestInfo info,
      @ResponsiveConfigParam final Map<String, Object> props
  ) {
    name = info.getDisplayName().replace("()", "");

    final String mongoConnection = (String) props.get(MONGO_ENDPOINT_CONFIG);
    client = SessionUtil.connect(mongoConnection, null, null, "", null);

    partitioner = new WindowSegmentPartitioner(10_000L, 1_000L, false);
    segment = partitioner.segmenter().activeSegments(0, 100).get(0);

    table = new MongoWindowTable(client, name, partitioner, true, UNSHARDED);
    flushManager = table.init(0);
    flushManager.updateOffsetAndStreamTime(0, 100);
    flushManager.createSegment(segment);
  }

  @Test
  public void shouldSucceedSimpleSetGet() {
    // Given:
    final WindowSegmentPartitioner partitioner =
        new WindowSegmentPartitioner(10_000L, 1_000L, false);
    final var segment = partitioner.segmenter().activeSegments(0, 100).get(0);

    final MongoWindowTable table =
        new MongoWindowTable(client, name, partitioner, false, UNSHARDED);

    final var flushManager = table.init(0);
    flushManager.updateOffsetAndStreamTime(0, 100);
    flushManager.createSegment(segment);

    // When:
    final var byteKey = Bytes.wrap("key".getBytes());
    final byte[] valueByte = new byte[] {1};
    var writer = flushManager.createWriter(segment, 0);
    writer.insert(
        new WindowedKey(byteKey, 0),
        valueByte,
        table.localEpoch(0)
    );
    writer.flush();

    // Then:
    var value = table.fetch(0, byteKey, 0);
    assertThat(value, Matchers.equalTo(valueByte));
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

    final MongoWindowTable table =
        new MongoWindowTable(client, name, partitioner, false, UNSHARDED);
    final var flushManager = table.init(0);
    flushManager.updateOffsetAndStreamTime(0, 6_000);
    flushManager.createSegment(segment);

    // When:
    final var byteKey = Bytes.wrap("key".getBytes());
    final byte[] byteValue = new byte[] {1};
    final var windowedKey1 = new WindowedKey(byteKey, 500);
    final var windowedKey2 = new WindowedKey(byteKey, 5_000);
    var writer = flushManager.createWriter(segment, 0);
    writer.insert(
        windowedKey1,
        byteValue,
        table.localEpoch(0)
    );
    writer.insert(
        windowedKey2,
        byteValue,
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
    assertThat(kvs.get(0).value, Matchers.equalTo(byteValue));

    assertThat(kvs.get(1).key.key, Matchers.equalTo(windowedKey2.key));
    assertThat(kvs.get(1).key.windowStartMs, Matchers.equalTo(windowedKey2.windowStartMs));
    assertThat(kvs.get(1).value, Matchers.equalTo(byteValue));
  }

  @Test
  public void shouldFetchInserts() {
    // given:
    final var writer = flushManager.createWriter(segment, 0);
    final Bytes key = Bytes.wrap("key".getBytes());
    writer.insert(new WindowedKey(key, 100), "val".getBytes(), 110);
    writer.flush();

    // when:
    final var val = table.fetch(0, Bytes.wrap("key".getBytes()), 100);

    // then:
    assertThat(val, equalsByteArray("val".getBytes()));
  }

  private void shouldReturnWindowsForFetch() {
    // given:
    final var writer = flushManager.createWriter(segment, 0);
    final Bytes key = Bytes.wrap("key".getBytes());
    final Bytes keyOther = Bytes.wrap("keyOther".getBytes());
    writer.insert(new WindowedKey(key, 99), "valEarly".getBytes(), 110);
    writer.insert(new WindowedKey(key, 100), "val".getBytes(), 110);
    writer.insert(new WindowedKey(keyOther, 105), "valOther".getBytes(), 110);
    writer.insert(new WindowedKey(key, 200), "val2".getBytes(), 210);
    writer.insert(new WindowedKey(key, 201), "valLate".getBytes(), 210);
    writer.flush();

    // when:
    final var iterator = table.fetch(0, key, 100, 200);

    // then:
    assertThat(iterator.hasNext(), is(true));
    assertThat(
        iterator.next(),
        sameKeyValue(new KeyValue<>(new WindowedKey(key, 100), "val".getBytes()))
    );
    assertThat(iterator.hasNext(), is(true));
    assertThat(
        iterator.next(),
        sameKeyValue(new KeyValue<>(new WindowedKey(key, 200), "val2".getBytes()))
    );
    assertThat(iterator.hasNext(), is(false));
    iterator.close();
  }

  @Test
  public void shouldFetchWhenTimestampFirstEnabled() {
    assertThat(table.isTimestampFirstOrder(), is(true));
    shouldReturnWindowsForFetch();
  }

  @Test
  public void shouldFetchWhenTimestampFirstDisabled() {
    table = new MongoWindowTable(client, name, partitioner, false, UNSHARDED);
    flushManager = table.init(0);
    flushManager.updateOffsetAndStreamTime(0, 100);
    flushManager.createSegment(segment);

    shouldReturnWindowsForFetch();
  }

  private void shouldReturnWindowsForFetchRange() {
    // given:
    final var writer = flushManager.createWriter(segment, 0);
    final Bytes key = Bytes.wrap("key".getBytes()); // key out of range
    final Bytes keyA = Bytes.wrap("keyA".getBytes());
    final Bytes keyAA = Bytes.wrap("keyAA".getBytes());
    final Bytes keyAB = Bytes.wrap("keyAB".getBytes());
    final Bytes keyB = Bytes.wrap("keyB".getBytes());
    final Bytes keyBA = Bytes.wrap("keyBA".getBytes()); // key out of range

    writer.insert(new WindowedKey(key, 100), "val".getBytes(), 110); // key out of range
    writer.insert(new WindowedKey(keyA, 99), "valAEarly".getBytes(), 110); // time out of range
    writer.insert(new WindowedKey(keyA, 100), "valA".getBytes(), 110);
    writer.insert(new WindowedKey(keyAA, 99), "valAAEarly".getBytes(), 110); // time out of range
    writer.insert(new WindowedKey(keyAA, 100), "valAA".getBytes(), 110);
    writer.insert(new WindowedKey(keyAB, 105), "valAB".getBytes(), 110);
    writer.insert(new WindowedKey(keyB, 200), "valB".getBytes(), 210);
    writer.insert(new WindowedKey(keyB, 201), "valBLate".getBytes(), 210); // time out of range
    writer.insert(new WindowedKey(keyBA, 200), "valBA".getBytes(), 210); // key out of range
    writer.flush();

    // when:
    final var iterator = table.fetchRange(0, keyA, keyB, 100, 200);

    // then:
    assertThat(iterator.hasNext(), is(true));
    assertThat(
        iterator.next(),
        sameKeyValue(new KeyValue<>(new WindowedKey(keyA, 100), "valA".getBytes()))
    );
    assertThat(iterator.hasNext(), is(true));
    assertThat(
        iterator.next(),
        sameKeyValue(new KeyValue<>(new WindowedKey(keyAA, 100), "valAA".getBytes()))
    );
    assertThat(iterator.hasNext(), is(true));
    assertThat(
        iterator.next(),
        sameKeyValue(new KeyValue<>(new WindowedKey(keyAB, 105), "valAB".getBytes()))
    );
    assertThat(iterator.hasNext(), is(true));
    assertThat(
        iterator.next(),
        sameKeyValue(new KeyValue<>(new WindowedKey(keyB, 200), "valB".getBytes()))
    );
    assertThat(iterator.hasNext(), is(false));
    iterator.close();
  }

  @Test
  public void shouldFetchRangeWhenTimestampFirstEnabled() {
    assertThat(table.isTimestampFirstOrder(), is(true));
    shouldReturnWindowsForFetchRange();
  }

  @Test
  public void shouldFetchRangeWhenTimestampFirstDisabled() {
    table = new MongoWindowTable(client, name, partitioner, false, UNSHARDED);
    flushManager = table.init(0);
    flushManager.updateOffsetAndStreamTime(0, 100);
    flushManager.createSegment(segment);

    shouldReturnWindowsForFetchRange();
  }

  private void shouldReturnWindowsForFetchAll() {
    // given:
    final var writer = flushManager.createWriter(segment, 0);
    final Bytes key = Bytes.wrap("key".getBytes());
    final Bytes keyA = Bytes.wrap("keyA".getBytes());
    final Bytes keyAA = Bytes.wrap("keyAA".getBytes());
    final Bytes keyAB = Bytes.wrap("keyAB".getBytes());
    final Bytes keyB = Bytes.wrap("keyB".getBytes());
    final Bytes keyBA = Bytes.wrap("keyBA".getBytes());

    writer.insert(new WindowedKey(key, 100), "val".getBytes(), 110);
    writer.insert(new WindowedKey(keyA, 99), "valAEarly".getBytes(), 110); // time out of range
    writer.insert(new WindowedKey(keyA, 100), "valA".getBytes(), 110);
    writer.insert(new WindowedKey(keyAA, 99), "valAAEarly".getBytes(), 110); // time out of range
    writer.insert(new WindowedKey(keyAA, 100), "valAA".getBytes(), 110);
    writer.insert(new WindowedKey(keyAB, 105), "valAB".getBytes(), 110);
    writer.insert(new WindowedKey(keyB, 200), "valB".getBytes(), 210);
    writer.insert(new WindowedKey(keyB, 201), "valBLate".getBytes(), 210); // time out of range
    writer.insert(new WindowedKey(keyBA, 200), "valBA".getBytes(), 210);
    writer.flush();

    // when:
    final var iterator = table.fetchAll(0, 100, 200);

    // then:
    assertThat(iterator.hasNext(), is(true));
    assertThat(
        iterator.next(),
        sameKeyValue(new KeyValue<>(new WindowedKey(key, 100), "val".getBytes()))
    );
    assertThat(iterator.hasNext(), is(true));
    assertThat(
        iterator.next(),
        sameKeyValue(new KeyValue<>(new WindowedKey(keyA, 100), "valA".getBytes()))
    );
    assertThat(iterator.hasNext(), is(true));
    assertThat(
        iterator.next(),
        sameKeyValue(new KeyValue<>(new WindowedKey(keyAA, 100), "valAA".getBytes()))
    );
    assertThat(iterator.hasNext(), is(true));
    assertThat(
        iterator.next(),
        sameKeyValue(new KeyValue<>(new WindowedKey(keyAB, 105), "valAB".getBytes()))
    );
    assertThat(iterator.hasNext(), is(true));
    assertThat(
        iterator.next(),
        sameKeyValue(new KeyValue<>(new WindowedKey(keyB, 200), "valB".getBytes()))
    );
    assertThat(iterator.hasNext(), is(true));
    assertThat(
        iterator.next(),
        sameKeyValue(new KeyValue<>(new WindowedKey(keyBA, 200), "valBA".getBytes()))
    );
    assertThat(iterator.hasNext(), is(false));
    iterator.close();
  }

  @Test
  public void shouldFetchAllWhenTimestampFirstEnabled() {
    assertThat(table.isTimestampFirstOrder(), is(true));
    shouldReturnWindowsForFetchAll();
  }

  @Test
  public void shouldFetchAllWhenTimestampFirstDisabled() {
    table = new MongoWindowTable(client, name, partitioner, false, UNSHARDED);
    flushManager = table.init(0);
    flushManager.updateOffsetAndStreamTime(0, 100);
    flushManager.createSegment(segment);

    shouldReturnWindowsForFetchAll();
  }

  @Test
  public void shouldEncodeKeyAsBase64String() {
    // given:
    final var writer = flushManager.createWriter(segment, 0);
    final Bytes key = Bytes.wrap("key".getBytes());
    writer.insert(new WindowedKey(key, 100), "val".getBytes(), 110);
    writer.flush();
    final var collection = table.windowsForSegmentPartition(0, segment);

    // when:
    final var result = collection.find(Filters.eq(
        String.join(".", WindowDoc.ID, WindowDoc.ID_RECORD_KEY),
        new StringKeyCodec().encode(key)
    )).first();

    // then:
    assertThat(result, not(nullValue()));
  }

  private static Matcher<byte[]> equalsByteArray(final byte[] expected) {
    return new EqualByteArray(expected);
  }

  private static class EqualByteArray extends BaseMatcher<byte[]> {
    private final byte[] expected;

    public EqualByteArray(byte[] expected) {
      this.expected = expected;
    }

    @Override
    public boolean matches(Object actual) {
      return actual instanceof byte[] && Arrays.equals(expected, (byte[]) actual);
    }

    @Override
    public void describeTo(Description description) {
      description.appendValue(expected);
    }
  }
}
