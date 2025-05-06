package dev.responsive.examples.rs3.demo;

import java.util.List;
import java.util.Random;

public class OrderGen {

  private final Random random;
  private final int numCustomers;
  private final int maxItemCost;

  private static final List<OrderGen.Department> departments = List.of(
      new OrderGen.Department("books", "94132"),
      new OrderGen.Department("electronics", "72326"),
      new OrderGen.Department("home", "52915"),
      new OrderGen.Department("apparel", "62524")
  );

  public OrderGen(
      final Random random,
      final int numCustomers,
      final int maxItemCost
  ) {
    this.random = random;
    this.numCustomers = numCustomers;
    this.maxItemCost = maxItemCost;
  }

  public Order next() {
    final var department = departments.get(Math.abs(random.nextInt(departments.size())));
    return new Order(
        nextOrderId(),
        nextCustomerId(),
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

  private String nextCustomerId() {
    return "customer_" + (Math.abs(random.nextLong()) % numCustomers);
  }

  private record Department(String name, String id) {}
}