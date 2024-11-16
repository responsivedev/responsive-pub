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

package dev.responsive.examples.common;

public class InjectedE2ETestException extends RuntimeException {
  private static final long serialVersionUID = 0L;

  public InjectedE2ETestException() {
    super("injected e2e test exception");
  }
}
