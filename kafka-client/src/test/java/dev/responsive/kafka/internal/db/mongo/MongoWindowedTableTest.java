/*
 * Copyright 2024 Responsive Computing, Inc.
 *
 * This source code is licensed under the Responsive Software License Agreement v1.0 available at
 *
 * https://www.responsive.dev/legal/responsive-bsl-10
 *
 * This software requires a valid Commercial License Key for production use. Trial and commercial
 * licenses can be obtained at https://www.responsive.dev/sdk/get-started.
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
import java.util.Arrays;
import java.util.Map;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.RegisterExtension;

class MongoWindowedTableTest {
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
  private MongoWindowedTable table;
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

    table = new MongoWindowedTable(client, name, partitioner, true, UNSHARDED);
    flushManager = table.init(0);
    flushManager.updateOffsetAndStreamTime(0, 100);
    flushManager.createSegment(segment);
  }

  @Test
  public void shouldFetchInserts() {
    // given:
    final var writer = flushManager.createWriter(segment);
    final Bytes key = Bytes.wrap("key".getBytes());
    writer.insert(new WindowedKey(key, 100), "val".getBytes(), 110);
    writer.flush();

    // when:
    final var val = table.fetch(0, Bytes.wrap("key".getBytes()), 100);

    // then:
    assertThat(val, equalsByteArray("val".getBytes()));
  }

  private void shouldFetchWindowsInRange() {
    // given:
    final var writer = flushManager.createWriter(segment);
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
  public void shouldFetchWindowsInRangeWhenTimestampFirstEnabled() {
    assertThat(table.isTimestampFirstOrder(), is(true));
    shouldFetchWindowsInRange();
  }

  @Test
  public void shouldFetchWindowsInRangeWhenTimestampFirstDisabled() {
    table = new MongoWindowedTable(client, name, partitioner, false, UNSHARDED);
    flushManager = table.init(0);
    flushManager.updateOffsetAndStreamTime(0, 100);
    flushManager.createSegment(segment);

    shouldFetchWindowsInRange();
  }

  @Test
  public void shouldEncodeKeyAsBase64String() {
    // given:
    final var writer = flushManager.createWriter(segment);
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