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

import dev.responsive.kafka.internal.db.rs3.client.Range;
import dev.responsive.kafka.internal.db.rs3.client.RangeBound;
import dev.responsive.rs3.Rs3;
import java.nio.charset.StandardCharsets;

public class GrpsRs3TestUtil {

  public static Rs3.RangeResult newKeyValueResult(String key) {
    final var keyValue = GrpcRs3Util.basicKeyValueProto(
        key.getBytes(StandardCharsets.UTF_8),
        "dummy".getBytes(StandardCharsets.UTF_8)
    );
    return Rs3.RangeResult.newBuilder()
        .setType(Rs3.RangeResult.Type.RESULT)
        .setResult(Rs3.KeyValue.newBuilder().setDefaultKv(keyValue))
        .build();
  }


  public static Rs3.RangeResult newEndOfStreamResult() {
    return Rs3.RangeResult.newBuilder()
        .setType(Rs3.RangeResult.Type.END_OF_STREAM)
        .build();
  }

  public static Range newRangeFromProto(Rs3.RangeRequest req) {
    final var range = req.getRange().getDefaultRange();
    final var startBound = newRangeBoundFromProto(range.getFrom());
    final var endBound = newRangeBoundFromProto(range.getTo());
    return new Range(startBound, endBound);
  }

  private static RangeBound newRangeBoundFromProto(Rs3.DefaultBound bound) {
    switch (bound.getType()) {
      case EXCLUSIVE:
        return RangeBound.exclusive(bound.getKey().getKey().toByteArray());
      case INCLUSIVE:
        return RangeBound.inclusive(bound.getKey().getKey().toByteArray());
      case UNBOUNDED:
        return RangeBound.unbounded();
      default:
        throw new IllegalArgumentException(String.format("Unknown range type %s", bound.getType()));
    }
  }

}
