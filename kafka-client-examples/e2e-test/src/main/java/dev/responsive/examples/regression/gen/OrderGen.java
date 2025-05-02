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

package dev.responsive.examples.regression.gen;


import dev.responsive.examples.e2etest.UrandomGenerator;
import dev.responsive.examples.regression.model.Order;

public class OrderGen {

  private final UrandomGenerator random;
  private final CustomerGen customerGen;
  private final int maxItemCost;

  public OrderGen(
      final UrandomGenerator random,
      final CustomerGen customerGen,
      final int maxItemCost
  ) {
    this.random = random;
    this.customerGen = customerGen;
    this.maxItemCost = maxItemCost;
  }

  public Order next() {
    // TODO(agavra): we can still send orders for tombstoned customer ids
    return new Order(
        nextOrderId(),
        customerGen.validCustomerId(),
        Math.abs(random.nextInt(maxItemCost) + random.nextInt(100) * .01)
    );
  }

  private String nextOrderId() {
    // generates a unique id for each order that still has some semantic meaning
    // the last random int is used to avoid collisions
    return "order_" + System.currentTimeMillis() + "_" + Math.abs(random.nextInt(1000));
  }

}
