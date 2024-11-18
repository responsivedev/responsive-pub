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

package dev.responsive.examples.common;

public enum Mode {
  DRIVER,
  APPLICATION,

  REGRESSION_ST_DRIVER,
  REGRESSION_ST_JOIN,
  REGRESSION_ST_BASELINE,

  REGRESSION_BATCH_DRIVER,
  REGRESSION_BATCH,
  REGRESSION_BATCH_BASELINE,
}
