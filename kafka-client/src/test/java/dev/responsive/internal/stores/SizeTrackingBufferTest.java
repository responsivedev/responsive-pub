package dev.responsive.internal.stores;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import dev.responsive.internal.db.KeySpec;
import dev.responsive.internal.utils.Result;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.utils.Bytes;
import org.junit.jupiter.api.Test;

class SizeTrackingBufferTest {

  private static final long TIMESTAMP = 100L;

  private final SizeTrackingBuffer<Bytes> buffer = new SizeTrackingBuffer<>(new KeySpec<>() {
    @Override
    public Bytes keyFromRecord(final ConsumerRecord<byte[], byte[]> record) {
      return Bytes.wrap(record.key());
    }

    @Override
    public Bytes bytes(final Bytes key) {
      return key;
    }

    @Override
    public int compare(final Bytes o1, final Bytes o2) {
      return o1.compareTo(o2);
    }
  });

  @Test
  public void shouldReturnSizeZeroOnEmptyBuffer() {
    assertThat(buffer.getBytes(), is(0L));
  }

  @Test
  public void shouldTrackSizeOnInserts() {
    // given:
    final List<SizeCase> ops = List.of(
        new SizeCase((byte) 1, 10, 20, 38),
        new SizeCase((byte) 2, 8, 9, 63),
        new SizeCase((byte) 3, 3, 7, 81)
    );

    // when/then:
    for (int i = 0; i < ops.size(); i++) {
      final var op = ops.get(i);
      final Bytes k = Bytes.wrap(bytes(op.key, op.keySize));
      final byte[] v = bytes((byte) i, op.recordSize);
      buffer.put(k, Result.value(k, v, TIMESTAMP));
      assertThat(buffer.getBytes(), is(op.expectedTotal));
    }
  }

  @Test
  public void shouldTrackSizeOnInsertsWithUpdates() {
    // given:
    final List<SizeCase> ops = List.of(
        new SizeCase((byte) 1, 10, 20, 38),
        new SizeCase((byte) 2, 8, 9, 63),
        new SizeCase((byte) 2, 3, 9, 58)
    );

    // when/then:
    for (int i = 0; i < ops.size(); i++) {
      final var op = ops.get(i);
      final Bytes k = Bytes.wrap(bytes(op.key, op.keySize));
      final byte[] v = bytes((byte) i, op.recordSize);
      buffer.put(k, Result.value(k, v, TIMESTAMP));
      assertThat(buffer.getBytes(), is(op.expectedTotal));
    }
  }

  @Test
  public void shouldTrackSizeOnTombstone() {
    // given:
    final Bytes k1 = Bytes.wrap(bytes((byte) 1, 10));

    // when:
    buffer.put(k1, Result.tombstone(k1, TIMESTAMP));

    // then:
    assertThat(buffer.getBytes(), is(18L));
  }

  @Test
  public void shouldTrackSizeOnUpdateTombstone() {
    // given:
    final Bytes k1 = Bytes.wrap(bytes((byte) 1, 10));
    buffer.put(k1, Result.tombstone(k1, TIMESTAMP));

    when:
    buffer.put(k1, Result.value(k1, bytes((byte) 2, 15), TIMESTAMP));

    // then:
    assertThat(buffer.getBytes(), is(33L));
  }

  @Test
  public void shouldTrackSizeOnUpdateFromTombstone() {
    // given:
    final Bytes k1 = Bytes.wrap(bytes((byte) 1, 10));
    buffer.put(k1, Result.value(k1, bytes((byte) 2, 15), TIMESTAMP));

    when:
    buffer.put(k1, Result.tombstone(k1, TIMESTAMP));

    // then:
    assertThat(buffer.getBytes(), is(18L));
  }


  @Test
  public void shouldTrackSizeOnClear() {
    // given:
    final Bytes k1 = Bytes.wrap(bytes((byte) 1, 10));
    buffer.put(k1, Result.value(k1, bytes((byte) 1, 15), TIMESTAMP));
    final Bytes k2 = Bytes.wrap(bytes((byte) 2, 10));
    buffer.put(k2, Result.value(k2, bytes((byte) 2, 15), TIMESTAMP));
    assertThat(buffer.getBytes(), is(66L));

    // when:
    buffer.clear();

    // then:
    assertThat(buffer.getBytes(), is(0L));
  }

  private byte[] bytes(final byte val, int length) {
    final byte[] ret = new byte[length];
    for (int i = 0; i < length; i++) {
      ret[i] = val;
    }
    return ret;
  }

  private static class SizeCase {
    byte key;
    int recordSize;
    int keySize;
    long expectedTotal;

    public SizeCase(byte key, int recordSize, int keySize, int expectedTotal) {
      this.key = key;
      this.recordSize = recordSize;
      this.keySize = keySize;
      this.expectedTotal = expectedTotal;
    }
  }
}