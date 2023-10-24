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

/**
 * A special kind of remote table that supports epoch-based fencing
 * via transactions or atomic write batches of some kind.
 */
public interface RemoteLwtTable<K, P, S> extends RemoteTable<K, P, S> {

  /**
   * @param tablePartition the table partition to fetch the epoch for
   *
   * @return the current epoch associated with this table partition
   */
  long fetchEpoch(
      final P tablePartition
  );

  /**
   * @param tablePartition  the table partition to reserve the epoch for
   * @param epoch           the intended epoch for this partition
   *
   * @return  a statement that, when executed, will "reserve" this epoch by
   *          attempting to set the epoch in the remote table metadata row
   *          corresponding to this kafka partition, and succeed if and only
   *          the existing epoch is strictly less than the passed-in {@code epoch}
   */
  S reserveEpoch(
      final P tablePartition,
      final long epoch
  );

  /**
   * @param tablePartition  the table partition to verify the epoch for
   * @param epoch           the expected epoch for this partition
   *
   * @return  a statement that, when executed, will "ensure" the current epoch is
   *          valid by attempting an update that will succeed if and only if the
   *          epoch in the metadata row of this remote table partition is exactly
   *          equal to the passed-in {@code epoch}
   */
  S ensureEpoch(
      final P tablePartition,
      final long epoch
  );

  default void advanceStreamTime(
      final int kafkaPartition,
      final long epoch
  ) {
    // Most underlying table implementations don't need to care about stream-time
  }
}
