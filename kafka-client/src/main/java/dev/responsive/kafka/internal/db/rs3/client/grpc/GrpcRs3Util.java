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

package dev.responsive.kafka.internal.db.rs3.client.grpc;

import dev.responsive.kafka.internal.db.rs3.client.RS3Exception;
import dev.responsive.kafka.internal.db.rs3.client.RS3TransientException;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;

public class GrpcRs3Util {

  public static RuntimeException wrapThrowable(Throwable t) {
    if (t instanceof StatusRuntimeException) {
      var statusRuntimeException = (StatusRuntimeException) t;
      if (statusRuntimeException.getStatus() == Status.UNAVAILABLE) {
        return new RS3TransientException(statusRuntimeException);
      } else {
        return new RS3Exception(statusRuntimeException);
      }
    } else if (t instanceof StatusException) {
      return new RS3Exception(t);
    } else if (t instanceof RuntimeException) {
      return (RuntimeException) t;
    } else {
      return new RuntimeException(t);
    }
  }
}
