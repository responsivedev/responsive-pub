/*
 * Copyright 2025 Responsive Computing, Inc.
 *
 * This source code is licensed under the Responsive Business Source License Agreement v1.0
 * available at:
 *
 * https://www.responsive.dev/legal/responsive-bsl-10
 *
 * This software requires a valid Commercial License Key for production use. Trial and commercial
 * licenses can be obtained at https://www.responsive.dev
 */

package dev.responsive.kafka.internal.db.rs3.client.grpc;

import com.google.protobuf.ByteString;
import dev.responsive.rs3.Rs3;

public class GrpsRs3TestUtil {

  public static Rs3.RangeResult newKeyValueResult(String key) {
    final var keyValue = Rs3.KeyValue.newBuilder()
        .setKey(ByteString.copyFromUtf8(key))
        .setValue(ByteString.copyFromUtf8("dummy"))
        .build();
    return Rs3.RangeResult.newBuilder()
        .setType(Rs3.RangeResult.Type.RESULT)
        .setResult(keyValue)
        .build();
  }


  public static Rs3.RangeResult newEndOfStreamResult() {
    return Rs3.RangeResult.newBuilder()
        .setType(Rs3.RangeResult.Type.END_OF_STREAM)
        .build();
  }

}
