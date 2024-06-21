package dev.responsive.kafka.internal.stores;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import dev.responsive.kafka.internal.db.BytesKeySpec;
import dev.responsive.kafka.internal.utils.Result;
import java.util.List;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.junit.jupiter.api.Test;

class SizeTrackingBufferTest {

  private static final long TIMESTAMP = 100L;
  private final BytesKeySpec keySpec = new BytesKeySpec();

  private final SizeTrackingBuffer<Bytes> uniqueKeyBuffer = new UniqueKeyBuffer<>(keySpec);
  private final SizeTrackingBuffer<Bytes> duplicatesBuffer = new DuplicateKeyBuffer<>(keySpec);

  @Test
  public void shouldReturnSizeZeroOnEmptyBuffer() {
    assertThat(uniqueKeyBuffer.sizeInBytes(), is(0L));
    assertThat(uniqueKeyBuffer.sizeInRecords(), is(0));

    assertThat(duplicatesBuffer.sizeInBytes(), is(0L));
    assertThat(duplicatesBuffer.sizeInRecords(), is(0));
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

      uniqueKeyBuffer.put(k, Result.value(k, v, TIMESTAMP));
      assertThat(uniqueKeyBuffer.sizeInBytes(), is(op.expectedTotal));
      assertThat(uniqueKeyBuffer.sizeInRecords(), is(i + 1));

      duplicatesBuffer.put(k, Result.value(k, v, TIMESTAMP));
      assertThat(duplicatesBuffer.sizeInBytes(), is(op.expectedTotal));
      assertThat(duplicatesBuffer.sizeInRecords(), is(i + 1));
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
    long totalBytes = 0L;
    for (int i = 0; i < ops.size(); i++) {
      final var op = ops.get(i);
      final Bytes k = Bytes.wrap(bytes(op.key, op.keySize));
      final byte[] v = bytes((byte) i, op.recordSize);
      totalBytes += op.recordSize + op.keySize + 8;

      uniqueKeyBuffer.put(k, Result.value(k, v, TIMESTAMP));
      assertThat(uniqueKeyBuffer.sizeInBytes(), is(op.expectedTotal));

      duplicatesBuffer.put(k, Result.value(k, v, TIMESTAMP));
      assertThat(duplicatesBuffer.sizeInBytes(), is(totalBytes));
    }

    assertThat(uniqueKeyBuffer.sizeInRecords(), is(2));
    assertThat(duplicatesBuffer.sizeInRecords(), is(3));
  }

  @Test
  public void shouldTrackSizeOnTombstone() {
    // given:
    final Bytes k1 = Bytes.wrap(bytes((byte) 1, 10));

    // when:
    uniqueKeyBuffer.put(k1, Result.tombstone(k1, TIMESTAMP));
    duplicatesBuffer.put(k1, Result.tombstone(k1, TIMESTAMP));

    // then:
    assertThat(uniqueKeyBuffer.sizeInBytes(), is(18L));
    assertThat(uniqueKeyBuffer.sizeInRecords(), is(1));

    // should drop tombstones for duplicate key buffers
    assertThat(duplicatesBuffer.sizeInBytes(), is(0L));
    assertThat(duplicatesBuffer.sizeInRecords(), is(0));
  }

  @Test
  public void shouldTrackSizeOnUpdateTombstone() {
    // given:
    final Bytes k1 = Bytes.wrap(bytes((byte) 1, 10));
    uniqueKeyBuffer.put(k1, Result.tombstone(k1, TIMESTAMP));

    when:
    uniqueKeyBuffer.put(k1, Result.value(k1, bytes((byte) 2, 15), TIMESTAMP));
    duplicatesBuffer.put(k1, Result.value(k1, bytes((byte) 2, 15), TIMESTAMP));

    // then:
    assertThat(uniqueKeyBuffer.sizeInBytes(), is(33L));
    assertThat(uniqueKeyBuffer.sizeInRecords(), is(1));

    // should drop tombstones for duplicate key buffers
    assertThat(duplicatesBuffer.sizeInBytes(), is(33L));
    assertThat(duplicatesBuffer.sizeInRecords(), is(1));
  }

  @Test
  public void shouldTrackSizeOnUpdateFromTombstone() {
    // given:
    final Bytes k1 = Bytes.wrap(bytes((byte) 1, 10));
    uniqueKeyBuffer.put(k1, Result.value(k1, bytes((byte) 2, 15), TIMESTAMP));
    duplicatesBuffer.put(k1, Result.value(k1, bytes((byte) 2, 15), TIMESTAMP));

    // when:
    uniqueKeyBuffer.put(k1, Result.tombstone(k1, TIMESTAMP));
    duplicatesBuffer.put(k1, Result.tombstone(k1, TIMESTAMP));

    // then:
    assertThat(uniqueKeyBuffer.sizeInBytes(), is(18L));
    assertThat(uniqueKeyBuffer.sizeInRecords(), is(1));

    // should drop tombstones for duplicate key buffers
    assertThat(duplicatesBuffer.sizeInBytes(), is(33L));
    assertThat(duplicatesBuffer.sizeInRecords(), is(1));
  }

  @Test
  public void shouldTrackSizeOnClear() {
    // given:
    final Bytes k1 = Bytes.wrap(bytes((byte) 1, 10));
    final Bytes k2 = Bytes.wrap(bytes((byte) 2, 10));

    uniqueKeyBuffer.put(k1, Result.value(k1, bytes((byte) 1, 15), TIMESTAMP));
    uniqueKeyBuffer.put(k2, Result.value(k2, bytes((byte) 2, 15), TIMESTAMP));

    duplicatesBuffer.put(k1, Result.value(k1, bytes((byte) 1, 15), TIMESTAMP));
    duplicatesBuffer.put(k2, Result.value(k2, bytes((byte) 2, 15), TIMESTAMP));

    assertThat(uniqueKeyBuffer.sizeInBytes(), is(66L));
    assertThat(uniqueKeyBuffer.sizeInRecords(), is(2));

    assertThat(duplicatesBuffer.sizeInBytes(), is(66L));
    assertThat(duplicatesBuffer.sizeInRecords(), is(2));

    // when:
    uniqueKeyBuffer.clear();
    duplicatesBuffer.clear();

    // then:
    assertThat(uniqueKeyBuffer.sizeInBytes(), is(0L));
    assertThat(uniqueKeyBuffer.sizeInRecords(), is(0));

    assertThat(duplicatesBuffer.sizeInBytes(), is(0L));
    assertThat(duplicatesBuffer.sizeInRecords(), is(0));
  }

  @Test
  public void shouldReturnAllEntries() {
    // given:
    final Bytes k1 = Bytes.wrap(bytes((byte) 1, 10));
    final Bytes k2 = Bytes.wrap(bytes((byte) 2, 10));

    final List<KeyValue<Bytes, Result<Bytes>>> ops = List.of(
        new KeyValue<>(k1, Result.value(k1, bytes((byte) 1, 15), 0L)),
        new KeyValue<>(k2, Result.value(k2, bytes((byte) 1, 15), 0L)),
        new KeyValue<>(k1, Result.value(k1, bytes((byte) 1, 15), 0L))
    );

    // when:
    for (final var kv : ops) {
      uniqueKeyBuffer.put(kv.key, kv.value);
      duplicatesBuffer.put(kv.key, kv.value);
    }

    // then:
    final var uniqueKeyIter = uniqueKeyBuffer.all();
    final var duplicatesIter = duplicatesBuffer.all();

    assertThat(uniqueKeyIter.next(), equalTo(ops.get(2)));
    assertThat(uniqueKeyIter.next(), equalTo(ops.get(1)));
    assertThat(uniqueKeyIter.hasNext(), is(false));

    assertThat(duplicatesIter.next(), equalTo(ops.get(0)));
    assertThat(duplicatesIter.next(), equalTo(ops.get(2)));
    assertThat(duplicatesIter.next(), equalTo(ops.get(1)));
    assertThat(duplicatesIter.hasNext(), is(false));
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