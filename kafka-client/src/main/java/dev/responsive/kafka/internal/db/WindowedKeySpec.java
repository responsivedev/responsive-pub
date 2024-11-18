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

package dev.responsive.kafka.internal.db;

import static org.apache.kafka.streams.state.StateSerdes.TIMESTAMP_SIZE;

import dev.responsive.kafka.internal.utils.WindowedKey;
import java.nio.ByteBuffer;
import java.util.function.Predicate;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class WindowedKeySpec implements KeySpec<WindowedKey> {

  private final Predicate<WindowedKey> withinRetention;

  public WindowedKeySpec(final Predicate<WindowedKey> withinRetention) {
    this.withinRetention = withinRetention;
  }

  @Override
  public int sizeInBytes(final WindowedKey key) {
    return key.key.get().length + Long.BYTES;
  }

  @Override
  public WindowedKey keyFromRecord(final ConsumerRecord<byte[], byte[]> record) {
    final byte[] key = record.key();

    // the WindowKeySchema in Streams always encodes the changelog record keys by
    // concatenating the data key + windowStartTime + seqnum
    // the seqnum is just an arbitrary integer that unfortunately can't be avoided,
    // so we just need to make sure to stop reading 4 bytes from the end
    final int dataKeySize = key.length - TIMESTAMP_SIZE - 4;
    final byte[] dataKeyBytes = new byte[dataKeySize];
    System.arraycopy(key, 0, dataKeyBytes, 0, dataKeyBytes.length);

    final long windowStartTs = ByteBuffer.wrap(key).getLong(dataKeySize);

    return new WindowedKey(dataKeyBytes, windowStartTs);
  }

  @Override
  public boolean retain(final WindowedKey key) {
    return withinRetention.test(key);
  }
}
