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

import com.datastax.oss.driver.api.core.cql.BoundStatement;

/**
 * An add-on for a special kind of remote table that supports epoch-based fencing
 * via transactions or atomic write batches of some kind, such as LWT in C*
 * <p>
 * TODO: does it make sense to refactor this so it integrates with the EOS Mongo
 *  table, which uses an epoch per write rather than per table-partition like C*
 *  Similarly, we might want/need to split off the stream-time functionality 
 *  into a separate interface or completely pull it into the RemoteWindowedTable
 *  class for two reasons: for when we persist the stream-time in the remote
 *  table, and for the eventual MongoDB-based window store implementation.
 */
public interface TableMetadata<P> {

  /**
   * @param tablePartition  the table partition to reserve the epoch for
   * @param epoch           the intended epoch for this partition
   *
   * @return  a statement that, when executed, will "reserve" this epoch by
   *          attempting to set the epoch in the remote table metadata row
   *          corresponding to this kafka partition, and succeed if and only
   *          the existing epoch is strictly less than the passed-in {@code epoch}
   */
  BoundStatement reserveEpoch(
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
  BoundStatement ensureEpoch(
      final P tablePartition,
      final long epoch
  );

  default void preCommit(
      final int kafkaPartition,
      final long epoch
  ) {
    // Only needs to be implemented if the table needs to prepare for the commit in some way,
    // for example by creating/reserving new segments in window stores
  }

  default void postCommit(
      final int kafkaPartition,
      final long epoch
  ) {
    // Only needs to be implemented if the table needs to clean up after the commit in some way,
    // for example by expiring/removing old segments in window stores
  }
}
