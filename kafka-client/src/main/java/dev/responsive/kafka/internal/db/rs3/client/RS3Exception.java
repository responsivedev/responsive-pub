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

package dev.responsive.kafka.internal.db.rs3.client;

// exception types for interesting failures like unaligned wal segment of read offset check fail
public class RS3Exception extends RuntimeException {
  private static final long serialVersionUID = 0L;

  public RS3Exception(String message) {
    super(message);
  }

  public RS3Exception(final Throwable cause) {
    super(cause);
  }
}
