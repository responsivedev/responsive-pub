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

package dev.responsive.kafka.api.async.internals;

public class FatalAsyncException extends RuntimeException {

  private static final long serialVersionUID = -4075437528263657236L;

  public FatalAsyncException(final Throwable fatalException) {
    super(fatalException);
  }

  public FatalAsyncException(final String message, final Throwable fatalException) {
    super(message, fatalException);
  }

}
