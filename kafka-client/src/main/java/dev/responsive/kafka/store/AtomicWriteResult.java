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

package dev.responsive.kafka.store;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;

public class AtomicWriteResult {

  private final int partition;
  private final boolean applied;

  public static AtomicWriteResult success(final int partition) {
    return new AtomicWriteResult(partition, true);
  }

  public static AtomicWriteResult failure(final int partition) {
    return new AtomicWriteResult(partition, false);
  }

  public static AtomicWriteResult of(final int partition, final AsyncResultSet resp) {
    return resp.wasApplied() ? success(partition) : failure(partition);
  }

  private AtomicWriteResult(final int partition, final boolean applied) {
    this.partition = partition;
    this.applied = applied;
  }

  public int getPartition() {
    return partition;
  }

  public boolean wasApplied() {
    return applied;
  }
}
