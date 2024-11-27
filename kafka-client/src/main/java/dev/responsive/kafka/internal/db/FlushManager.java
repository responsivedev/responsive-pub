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

import dev.responsive.kafka.internal.db.partitioning.TablePartitioner;
import dev.responsive.kafka.internal.stores.RemoteWriteResult;

public interface FlushManager<K, P> {

  String tableName();

  TablePartitioner<K, P> partitioner();

  RemoteWriter<K, P> createWriter(final P tablePartition, final long consumedOffset);

  void writeAdded(final K key);

  RemoteWriteResult<P> preFlush();

  RemoteWriteResult<P> postFlush(final long consumedOffset);

  String failedFlushInfo(final long batchOffset, final P failedTablePartition);

  String logPrefix();

}
