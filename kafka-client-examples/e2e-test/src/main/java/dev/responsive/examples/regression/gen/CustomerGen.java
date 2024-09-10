/*
 * Copyright 2023 Responsive Computing, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
