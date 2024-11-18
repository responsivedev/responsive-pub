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

import dev.responsive.kafka.internal.utils.SessionKey;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueIterator;

public interface RemoteSessionTable<S> extends RemoteTable<SessionKey, S> {

  /**
   * Initializes the table by setting the metadata fields to
   * their initialized values.
   *
   * @return a {@link SessionFlushManager} that gives the callee access
   * to run statements on {@code table}
   */
  SessionFlushManager init(
      final int kafkaPartition
  );

  /**
   * Retrieves the value of the given {@code kafkaPartition} and {@code key} with
   * a session start time of {@code sessionStart} and a session end time of
   * {@code sessionEnd}.
   *
   * @param kafkaPartition the kafka partition
   * @param key            the data key
   * @param sessionStart   the start time of the session
   * @param sessionEnd     the start time of the session
   * @return the value previously set
   */
  byte[] fetch(
      int kafkaPartition,
      Bytes key,
      long sessionStart,
      long sessionEnd
  );

  /**
   * Retrieves the range of sessions of the given {@code kafkaPartition} and {@code key} with
   * an end time between {@code earliestSessionEnd} and {@code latestSessionEnd}.
   *
   * @param kafkaPartition     the kafka partition
   * @param key                the data key
   * @param earliestSessionEnd the earliest possible end time of the session
   * @param latestSessionEnd   the latest possible end time of the session
   * @return a forwards iterator over the retrieved sessions and values previously set.
   */
  KeyValueIterator<SessionKey, byte[]> fetchAll(
      final int kafkaPartition,
      final Bytes key,
      final long earliestSessionEnd,
      final long latestSessionEnd
  );
}
