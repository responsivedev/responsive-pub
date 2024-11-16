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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.utils.Bytes;

public class BytesKeySpec implements KeySpec<Bytes> {

  @Override
  public Bytes keyFromRecord(final ConsumerRecord<byte[], byte[]> record) {
    return Bytes.wrap(record.key());
  }

  @Override
  public int sizeInBytes(final Bytes key) {
    return key.get().length;
  }

}
