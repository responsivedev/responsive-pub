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

package dev.responsive.kafka.internal.db.inmemory;

import static dev.responsive.kafka.internal.stores.TtlResolver.NO_TTL;
import static dev.responsive.kafka.testutils.IntegrationTestUtils.defaultOnlyTtl;
import static dev.responsive.kafka.testutils.Matchers.sameKeyValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import dev.responsive.kafka.internal.db.KVFlushManager;
import dev.responsive.kafka.internal.db.RemoteWriter;
import java.time.Duration;
import org.apache.kafka.common.serialization.BytesSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.junit.jupiter.api.Test;

class InMemoryKVTableTest {
  private final InMemoryKVTable table = new InMemoryKVTable("name", NO_TTL);
  private final KVFlushManager flushManager =  table.init(0);
  private final RemoteWriter<Bytes, Integer> writer = flushManager.createWriter(0, 0);

  @Test
  public void shouldGetPuts() {
    // given:
    writer.insert(Bytes.wrap("key".getBytes()), "val".getBytes(), 100);
    writer.flush();

    // when/then:
    assertThat(table.get(0, Bytes.wrap("key".getBytes()), -1), is("val".getBytes()));
  }

  @Test
  public void shouldDoRangeScan() {
    // given:
    writer.insert(Bytes.wrap("aa".getBytes()), "val0".getBytes(), 100);
    writer.insert(Bytes.wrap("aaa".getBytes()), "val1".getBytes(), 100);
    writer.insert(Bytes.wrap("bbbb".getBytes()), "val2".getBytes(), 100);
    writer.insert(Bytes.wrap("cc".getBytes()), "val3".getBytes(), 100);
    writer.insert(Bytes.wrap("ccddd".getBytes()), "val4".getBytes(), 100);
    writer.flush();

    // when:
    final var iter = table.range(
        0,
        Bytes.wrap("aaa".getBytes()),
        Bytes.wrap("cc".getBytes()),
        0
    );

    // then:
    assertThat(
        iter.next(),
        sameKeyValue(new KeyValue<>(Bytes.wrap("aaa".getBytes()), "val1".getBytes()))
    );
    assertThat(
        iter.next(),
        sameKeyValue(new KeyValue<>(Bytes.wrap("bbbb".getBytes()), "val2".getBytes()))
    );
    assertThat(
        iter.next(),
        sameKeyValue(new KeyValue<>(Bytes.wrap("cc".getBytes()), "val3".getBytes()))
    );
    assertThat(iter.hasNext(), is(false));
    iter.close();
  }

  @Test
  public void shouldDoRangeScanWithNullFrom() {
    // given:
    writer.insert(Bytes.wrap("aa".getBytes()), "val0".getBytes(), 100);
    writer.insert(Bytes.wrap("aaa".getBytes()), "val1".getBytes(), 100);
    writer.insert(Bytes.wrap("bbbb".getBytes()), "val2".getBytes(), 100);
    writer.insert(Bytes.wrap("cc".getBytes()), "val3".getBytes(), 100);
    writer.insert(Bytes.wrap("ccddd".getBytes()), "val4".getBytes(), 100);
    writer.flush();

    // when:
    final var iter = table.range(
        0,
        null,
        Bytes.wrap("cc".getBytes()),
        0
    );

    // then:
    assertThat(
        iter.next(),
        sameKeyValue(new KeyValue<>(Bytes.wrap("aa".getBytes()), "val0".getBytes()))
    );
    assertThat(
        iter.next(),
        sameKeyValue(new KeyValue<>(Bytes.wrap("aaa".getBytes()), "val1".getBytes()))
    );
    assertThat(
        iter.next(),
        sameKeyValue(new KeyValue<>(Bytes.wrap("bbbb".getBytes()), "val2".getBytes()))
    );
    assertThat(
        iter.next(),
        sameKeyValue(new KeyValue<>(Bytes.wrap("cc".getBytes()), "val3".getBytes()))
    );
    assertThat(iter.hasNext(), is(false));
    iter.close();
  }

  @Test
  public void shouldDoRangeScanWithNullTo() {
    // given:
    writer.insert(Bytes.wrap("aa".getBytes()), "val0".getBytes(), 100);
    writer.insert(Bytes.wrap("aaa".getBytes()), "val1".getBytes(), 100);
    writer.insert(Bytes.wrap("bbbb".getBytes()), "val2".getBytes(), 100);
    writer.insert(Bytes.wrap("cc".getBytes()), "val3".getBytes(), 100);
    writer.insert(Bytes.wrap("ccddd".getBytes()), "val4".getBytes(), 100);
    writer.flush();

    // when:
    final var iter = table.range(
        0,
        Bytes.wrap("aaa".getBytes()),
        null,
        0
    );

    // then:
    assertThat(
        iter.next(),
        sameKeyValue(new KeyValue<>(Bytes.wrap("aaa".getBytes()), "val1".getBytes()))
    );
    assertThat(
        iter.next(),
        sameKeyValue(new KeyValue<>(Bytes.wrap("bbbb".getBytes()), "val2".getBytes()))
    );
    assertThat(
        iter.next(),
        sameKeyValue(new KeyValue<>(Bytes.wrap("cc".getBytes()), "val3".getBytes()))
    );
    assertThat(
        iter.next(),
        sameKeyValue(new KeyValue<>(Bytes.wrap("ccddd".getBytes()), "val4".getBytes()))
    );
    assertThat(iter.hasNext(), is(false));
    iter.close();
  }

  @Test
  public void shouldDoPrefixScan() {
    // given:
    writer.insert(bytes(1, 2), new byte[]{0x1}, 100);
    writer.insert(bytes(2), new byte[]{0x1}, 100);
    writer.insert(bytes(2, 2), new byte[]{0x1}, 100);
    writer.insert(bytes(2, 2, 3), new byte[]{0x1}, 100);
    writer.insert(bytes(3), new byte[]{0x1}, 100);
    writer.flush();

    // when:
    final var iter = table.prefix(bytes(2), new BytesSerializer(), 0, 0);

    // then:
    assertThat(iter.next(), sameKeyValue(new KeyValue<>(bytes(2), new byte[] {0x1})));
    assertThat(iter.next(), sameKeyValue(new KeyValue<>(bytes(2, 2), new byte[] {0x1})));
    assertThat(iter.next(), sameKeyValue(new KeyValue<>(bytes(2, 2, 3), new byte[] {0x1})));
    assertThat(iter.hasNext(), is(false));
    iter.close();
  }

  @Test
  public void shouldDoPrefixScanWithOverflow() {
    // given:
    writer.insert(bytes(-1, -1, -1), new byte[]{0x1}, 100);
    writer.insert(bytes(-1), new byte[]{0x1}, 100);
    writer.flush();

    // when:
    final var iter = table.prefix(bytes(-1), new BytesSerializer(), 0, 0);

    // then:
    assertThat(iter.next(), sameKeyValue(new KeyValue<>(bytes(-1), new byte[] {0x1})));
    assertThat(iter.next(), sameKeyValue(new KeyValue<>(bytes(-1, -1, -1), new byte[] {0x1})));
    assertThat(iter.hasNext(), is(false));
    iter.close();
  }

  @Test
  public void shouldDoFullScan() {
    writer.insert(Bytes.wrap("aaa".getBytes()), "val1".getBytes(), 100);
    writer.insert(Bytes.wrap("bbbb".getBytes()), "val2".getBytes(), 100);
    writer.insert(Bytes.wrap("cc".getBytes()), "val3".getBytes(), 100);
    writer.insert(Bytes.wrap("ccddd".getBytes()), "val4".getBytes(), 100);
    writer.flush();

    // when:
    final var iter = table.all(0, -1);

    // then:
    assertThat(
        iter.next(),
        sameKeyValue(new KeyValue<>(Bytes.wrap("aaa".getBytes()), "val1".getBytes()))
    );
    assertThat(
        iter.next(),
        sameKeyValue(new KeyValue<>(Bytes.wrap("bbbb".getBytes()), "val2".getBytes()))
    );
    assertThat(
        iter.next(),
        sameKeyValue(new KeyValue<>(Bytes.wrap("cc".getBytes()), "val3".getBytes()))
    );
    assertThat(
        iter.next(),
        sameKeyValue(new KeyValue<>(Bytes.wrap("ccddd".getBytes()), "val4".getBytes()))
    );
    assertThat(iter.hasNext(), is(false));
    iter.close();
  }

  @Test
  public void shouldFilterMinTimestampOnGet() {
    // given:
    final Duration ttl = Duration.ofMillis(300L);

    final InMemoryKVTable table = new InMemoryKVTable("name", defaultOnlyTtl(ttl));

    final KVFlushManager flushManager =  table.init(0);
    final RemoteWriter<Bytes, Integer> writer = flushManager.createWriter(0, 0);

    // when:
    writer.insert(Bytes.wrap("key".getBytes()), "val".getBytes(), 0);
    writer.flush();

    // then:
    assertThat(table.get(0, Bytes.wrap("key".getBytes()), 301), is(nullValue()));
  }

  @Test
  public void shouldFilterMinTimestampOnRangeScan() {
    // given:
    final Duration ttl = Duration.ofMillis(300L);

    final InMemoryKVTable table = new InMemoryKVTable("name", defaultOnlyTtl(ttl));
    final KVFlushManager flushManager =  table.init(0);
    final RemoteWriter<Bytes, Integer> writer = flushManager.createWriter(0, 0);

    writer.insert(Bytes.wrap("aaa".getBytes()), "val1".getBytes(), 100);
    writer.insert(Bytes.wrap("bbbb".getBytes()), "val2".getBytes(), 100);
    writer.insert(Bytes.wrap("cc".getBytes()), "val3".getBytes(), 500);
    writer.flush();

    // when:
    final var iter = table.range(
        0,
        Bytes.wrap("aaa".getBytes()),
        Bytes.wrap("cc".getBytes()),
        600
    );

    // then:
    assertThat(
        iter.next(),
        sameKeyValue(new KeyValue<>(Bytes.wrap("cc".getBytes()), "val3".getBytes()))
    );
    assertThat(iter.hasNext(), is(false));
    iter.close();
  }

  @Test
  public void shouldFilterMinTimestampOnFullScan() {
    // given:
    final Duration ttl = Duration.ofMillis(300L);

    final InMemoryKVTable table = new InMemoryKVTable("name", defaultOnlyTtl(ttl));
    final KVFlushManager flushManager =  table.init(0);
    final RemoteWriter<Bytes, Integer> writer = flushManager.createWriter(0, 0);

    writer.insert(Bytes.wrap("aaa".getBytes()), "val1".getBytes(), 100);
    writer.insert(Bytes.wrap("bbbb".getBytes()), "val2".getBytes(), 100);
    writer.insert(Bytes.wrap("cc".getBytes()), "val3".getBytes(), 500);
    writer.flush();

    // when:
    final var iter = table.all(0, 600);

    // then:
    assertThat(
        iter.next(),
        sameKeyValue(new KeyValue<>(Bytes.wrap("cc".getBytes()), "val3".getBytes()))
    );
    assertThat(iter.hasNext(), is(false));
    iter.close();
  }

  private byte[] byteArray(int... bytes) {
    byte[] byteArray = new byte[bytes.length];
    for (int i = 0; i < bytes.length; i++) {
      byteArray[i] = (byte) bytes[i];
    }
    return byteArray;
  }

  private Bytes bytes(int... bytes) {
    return Bytes.wrap(byteArray(bytes));
  }
}