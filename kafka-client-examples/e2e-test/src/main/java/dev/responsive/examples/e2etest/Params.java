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

package dev.responsive.examples.e2etest;

import dev.responsive.examples.common.Mode;

public class Params {
  public static final String NAME = System.getenv().getOrDefault("TEST_NAME", "e2e");
  public static final String INPUT_TOPIC = System.getenv().getOrDefault("INPUT_TOPIC", "input");
  public static final String OUTPUT_TOPIC = System.getenv().getOrDefault("OUTPUT_TOPIC", "output");
  public static final int PARTITIONS
      = Integer.parseInt(System.getenv().getOrDefault("PARTITIONS", "8"));
  public static final int EXCEPTION_INJECT_THRESHOLD
      = Integer.parseInt(System.getenv().getOrDefault("EXCEPTION_INJECT_THRESHOLD", "0"));
  public static final int NUM_KEYS
      = Integer.parseInt(System.getenv().getOrDefault("NUM_KEYS", "32"));
  public static final int MAX_OUTSTANDING
      = Integer.parseInt(System.getenv().getOrDefault("MAX_OUTSTANDING", "500"));
  public static final int RECEIVE_THRESHOLD
      = Integer.parseInt(System.getenv().getOrDefault("RECEIVE_THRESHOLD", "480"));
  public static final int FAULT_STOP_THRESHOLD
      = Integer.parseInt(System.getenv().getOrDefault("FAULT_STOP_THRESHOLD", "480"));
  public static final String MODE =
      System.getenv().getOrDefault("E2E_APP_MODE", Mode.APPLICATION.name());

}
