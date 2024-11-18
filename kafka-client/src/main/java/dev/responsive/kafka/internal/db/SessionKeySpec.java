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

import dev.responsive.kafka.internal.utils.SessionKey;
import java.nio.ByteBuffer;
import java.util.function.Predicate;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class SessionKeySpec implements KeySpec<SessionKey> {

  private final Predicate<SessionKey> withinRetention;

  public SessionKeySpec(final Predicate<SessionKey> withinRetention) {
    this.withinRetention = withinRetention;
  }

  @Override
  public SessionKey keyFromRecord(final ConsumerRecord<byte[], byte[]> record) {
    final byte[] key = record.key();
    return new SessionKey(
        extractKeyBytes(key),
        extractStartTimestamp(key),
        extractEndTimestamp(key)
    );
  }

  @Override
  public boolean retain(final SessionKey key) {
    return withinRetention.test(key);
  }

  /*
   * Inspired from KafkaStream internal functions within SessionKeySchema.
   */
  @Override
  public int sizeInBytes(final SessionKey key) {
    return (key == null ? 0 : key.key.get().length) + 2 * TIMESTAMP_SIZE;
  }

  /*
   * Inspired from KafkaStream internal functions within SessionKeySchema.
   */
  private static byte[] extractKeyBytes(final byte[] binaryKey) {
    final byte[] bytes = new byte[binaryKey.length - 2 * TIMESTAMP_SIZE];
    System.arraycopy(binaryKey, 0, bytes, 0, bytes.length);
    return bytes;
  }

  /*
   * Inspired from KafkaStream internal functions within SessionKeySchema.
   */
  private static long extractEndTimestamp(final byte[] binaryKey) {
    return ByteBuffer.wrap(binaryKey).getLong(binaryKey.length - 2 * TIMESTAMP_SIZE);
  }

  /*
   * Inspired from KafkaStream internal functions within SessionKeySchema.
   */
  private static long extractStartTimestamp(final byte[] binaryKey) {
    return ByteBuffer.wrap(binaryKey).getLong(binaryKey.length - TIMESTAMP_SIZE);
  }
}
