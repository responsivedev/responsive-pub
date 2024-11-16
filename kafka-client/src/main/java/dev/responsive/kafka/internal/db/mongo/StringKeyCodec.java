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

package dev.responsive.kafka.internal.db.mongo;

import org.apache.kafka.common.utils.Bytes;

public class StringKeyCodec implements KeyCodec {
  private final OrderPreservingBase64Encoder encoder = new OrderPreservingBase64Encoder();

  @Override
  public Bytes decode(final String key) {
    return Bytes.wrap(encoder.decode(key));
  }

  @Override
  public String encode(Bytes key) {
    return encoder.encode(key.get());
  }
}
