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

package dev.responsive.kafka.internal.stores;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;

public class RemoteWriteResult<P> {

  private final P tablePartition;
  private final boolean applied;

  public static <P> RemoteWriteResult<P> success(final P tablePartition) {
    return new RemoteWriteResult<P>(tablePartition, true);
  }

  public static <P> RemoteWriteResult<P> failure(final P tablePartition) {
    return new RemoteWriteResult<P>(tablePartition, false);
  }

  public static <P> RemoteWriteResult<P> of(final P tablePartition, final AsyncResultSet resp) {
    return resp.wasApplied() ? success(tablePartition) : failure(tablePartition);
  }

  private RemoteWriteResult(final P tablePartition, final boolean applied) {
    this.tablePartition = tablePartition;
    this.applied = applied;
  }

  public P tablePartition() {
    return tablePartition;
  }

  public boolean wasApplied() {
    return applied;
  }
}
