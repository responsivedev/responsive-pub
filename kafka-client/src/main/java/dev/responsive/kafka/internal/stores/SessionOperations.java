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

package dev.responsive.kafka.internal.stores;

import dev.responsive.kafka.internal.utils.SessionKey;
import java.io.Closeable;
import java.util.Collection;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;

public interface SessionOperations extends Closeable {

  long initialStreamTime();

  void put(final SessionKey key, final byte[] value);

  void delete(final SessionKey key);

  byte[] fetch(final SessionKey key);

  /**
   * Retrieves the range of sessions for the given {@code key} with
   * an end time between {@code earliestSessionEnd} and {@code latestSessionEnd}.
   *
   * @param key                the data key
   * @param earliestSessionEnd the earliest possible end time of the session
   * @param latestSessionEnd   the latest possible end time of the session
   * @return a forwards iterator over the retrieved sessions and values previously set.
   */
  KeyValueIterator<Windowed<Bytes>, byte[]> fetchAll(
      final Bytes key,
      final long earliestSessionEnd,
      final long latestSessionEnd
  );

  long restoreBatch(
      final Collection<ConsumerRecord<byte[], byte[]>> records,
      final long streamTimeMs
  );

  @Override
  void close();
}
