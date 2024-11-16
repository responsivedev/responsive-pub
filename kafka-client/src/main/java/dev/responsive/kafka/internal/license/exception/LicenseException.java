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

package dev.responsive.kafka.internal.license.exception;

public class LicenseException extends RuntimeException {
  private static final long serialVersionUID = 0L;

  public LicenseException(final String message) {
    super(message);
  }
}
