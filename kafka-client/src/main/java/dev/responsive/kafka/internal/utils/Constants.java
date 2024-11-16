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

package dev.responsive.kafka.internal.utils;

import java.util.concurrent.TimeUnit;

public final class Constants {
  public static final int BLOCKING_TIMEOUT_VALUE = 10;
  public static final TimeUnit BLOCKING_TIMEOUT_UNIT = TimeUnit.MINUTES;

  private Constants() {
  }
}
