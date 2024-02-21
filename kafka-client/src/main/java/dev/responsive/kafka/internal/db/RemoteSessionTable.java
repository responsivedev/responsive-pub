/*
 * Copyright 2023 Responsive Computing, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
   * @return a {@link WindowFlushManager} that gives the callee access
   * to run statements on {@code table}
   */
  SessionFlushManager init(
      final int kafkaPartition
  );

  /**
   * Retrieves the value of the given {@code kafkaPartition} and {@code key}.
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
   * an end time of at least {@code earliestSessionEnd} and a start time of at most
   * {@code latestSessionStart}.
   *
   * @param kafkaPartition     the kafka partition
   * @param key                the data key
   * @param earliestSessionEnd the earliest possible end time of the session
   * @param latestSessionStart the latest possible start time of the session
   * @return a forwards iterator over the retrieved sessions and values previously set.
   */
  KeyValueIterator<SessionKey, byte[]> fetchAll(
      final int kafkaPartition,
      final Bytes key,
      final long earliestSessionEnd,
      final long latestSessionStart
  );

  // TODO: Add more methods that will be used in SessionOperations.
}
