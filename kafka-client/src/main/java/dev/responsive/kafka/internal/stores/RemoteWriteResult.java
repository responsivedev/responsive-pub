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
