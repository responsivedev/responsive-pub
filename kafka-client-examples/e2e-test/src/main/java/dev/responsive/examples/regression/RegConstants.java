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

package dev.responsive.examples.regression;

public class RegConstants {
  public static final String ORDERS = "orders";
  public static final String CUSTOMER_ID_TO_NAME = "customer-names-to-id";
  public static final String CUSTOMER_NAME_TO_LOCATION = "customer-ids-to-location";
  public static final int NUM_PARTITIONS = 8;
  private static final String RESULTS = "enriched-orders";

  private RegConstants() { }

  public static String resultsTopic(final boolean responsive) {
    return RESULTS + (responsive ? "-responsive" : "-baseline");
  }

}
