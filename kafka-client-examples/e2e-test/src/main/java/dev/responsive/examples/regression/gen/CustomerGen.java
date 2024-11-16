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
import dev.responsive.examples.regression.model.Customer;
import java.util.ArrayList;
import java.util.List;

public class CustomerGen {

  public static final int NUM_CUSTOMERS = 10000;
  private static final String[] NAMES = {
      "James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda", "William",
      "Elizabeth", "David", "Barbara", "Richard", "Susan", "Joseph", "Jessica", "Thomas", "Sarah",
      "Charles", "Karen", "Christopher", "Nancy", "Daniel", "Lisa", "Matthew", "Margaret",
      "Anthony", "Betty", "Donald", "Sandra", "Mark", "Ashley", "Paul", "Dorothy", "Steven",
      "Kimberly", "Andrew", "Helen", "Joshua", "Donna", "Kevin", "Carol", "Brian", "Amanda",
      "George", "Melissa", "Edward", "Deborah", "Ronald", "Stephanie", "Timothy", "Rebecca",
      "Jason", "Sharon", "Jeffrey", "Laura", "Ryan", "Cynthia", "Jacob", "Kathleen", "Gary",
      "Angela", "Nicholas", "Shirley"
  };
  private static final String[] LOCATIONS = {
      "NY", "CA", "TX", "FL", "IL", "PA", "OH", "MI", "GA", "NJ"
  };

  private final UrandomGenerator random;
  private final List<String> customerIds = new ArrayList<>();

  public CustomerGen(final UrandomGenerator random) {
    this.random = random;
  }

  public Customer next() {
    return new Customer(
        nextCustomerId(random),
        NAMES[Math.abs(random.nextInt(NAMES.length))],
        LOCATIONS[Math.abs(random.nextInt(LOCATIONS.length))]
    );
  }

  private String nextCustomerId(final UrandomGenerator random) {
    final var id = "customer_" + Math.abs(random.nextInt(NUM_CUSTOMERS));
    customerIds.add(id);
    return id;
  }

  public String validCustomerId() {
    // TODO(agavra): remove tombstoned customer ids
    return customerIds.get(Math.abs(random.nextInt(customerIds.size())));
  }
}
