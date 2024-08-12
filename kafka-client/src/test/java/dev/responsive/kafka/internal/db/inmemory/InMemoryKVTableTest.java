package dev.responsive.kafka.internal.db.inmemory;

import static dev.responsive.kafka.internal.db.testutils.Matchers.sameKeyValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import dev.responsive.kafka.internal.db.KVFlushManager;
import dev.responsive.kafka.internal.db.RemoteWriter;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.junit.jupiter.api.Test;

class InMemoryKVTableTest {
  private final InMemoryKVTable table = new InMemoryKVTable("name");
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
    writer.insert(Bytes.wrap("key".getBytes()), "val".getBytes(), 100);
    writer.flush();

    // when/then:
    assertThat(table.get(0, Bytes.wrap("key".getBytes()), 101), is(nullValue()));
  }

  @Test
  public void shouldFilterMinTimestampOnRangeScan() {
    // given:
    writer.insert(Bytes.wrap("aaa".getBytes()), "val1".getBytes(), 100);
    writer.insert(Bytes.wrap("bbbb".getBytes()), "val2".getBytes(), 100);
    writer.insert(Bytes.wrap("cc".getBytes()), "val3".getBytes(), 500);
    writer.flush();

    // when:
    final var iter = table.range(
        0,
        Bytes.wrap("aaa".getBytes()),
        Bytes.wrap("cc".getBytes()),
        300
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
    writer.insert(Bytes.wrap("aaa".getBytes()), "val1".getBytes(), 100);
    writer.insert(Bytes.wrap("bbbb".getBytes()), "val2".getBytes(), 100);
    writer.insert(Bytes.wrap("cc".getBytes()), "val3".getBytes(), 500);
    writer.flush();

    // when:
    final var iter = table.all(0, 300);

    // then:
    assertThat(
        iter.next(),
        sameKeyValue(new KeyValue<>(Bytes.wrap("cc".getBytes()), "val3".getBytes()))
    );
    assertThat(iter.hasNext(), is(false));
    iter.close();
  }
}