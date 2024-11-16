/*
 * Copyright 2024 Responsive Computing, Inc.
 *
 * This source code is licensed under the Responsive Software License Agreement v1.0 available at
 *
 * https://www.responsive.dev/legal/responsive-bsl-10
 *
 * This software requires a valid Commercial License Key for production use. Trial and commercial
 * licenses can be obtained at https://www.responsive.dev/sdk/get-started.
 */

package dev.responsive.kafka.internal.db;

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import dev.responsive.kafka.internal.clients.TTDCassandraClient;

// TODO: use mock return values instead of null here and in the KV/Window child classes
public abstract class TTDTable<K> implements RemoteTable<K, BoundStatement> {

  protected final TTDCassandraClient client;

  public TTDTable(final TTDCassandraClient client) {
    this.client = client;
  }

  /**
   * @return the number of elements in this table
   *         or 0 if the schema has no such table
   */
  public abstract long count();

  @Override
  public long fetchOffset(final int kafkaPartition) {
    return 0;
  }

}
