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
import java.util.List;

public class OrderGen {

  private final UrandomGenerator random;
  private final CustomerGen customerGen;
  private final int maxItemCost;

  private static final List<Department> departments = List.of(
      new Department("books", "94132"),
      new Department("electronics", "72326"),
      new Department("home", "52915"),
      new Department("apparel", "62524")
  );

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
    final var department = departments.get(Math.abs(random.nextInt(departments.size())));
    return new Order(
        nextOrderId(),
        customerGen.validCustomerId(),
        department.name(),
        department.id(),
        Math.abs(random.nextInt(maxItemCost) + random.nextInt(100) * .01)
    );
  }

  private String nextOrderId() {
    // generates a unique id for each order that still has some semantic meaning
    // the last random int is used to avoid collisions
    return "order_" + System.currentTimeMillis() + "_" + Math.abs(random.nextInt(1000));
  }

  private record Department(String name, String id) {}
}
