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
import dev.responsive.examples.regression.model.Order;

public class OrderGen {

  private final UrandomGenerator random;
  private final CustomerGen customerGen;

  public OrderGen(final UrandomGenerator random, final CustomerGen customerGen) {
    this.random = random;
    this.customerGen = customerGen;
  }

  public Order next() {
    // TODO(agavra): we can still send orders for tombstoned customer ids
    return new Order(
        nextOrderId(),
        customerGen.validCustomerId(),
        random.nextInt(1000) + random.nextInt(100) * .01
    );
  }

  private String nextOrderId() {
    // generates a unique id for each order that still has some semantic meaning
    // the last random int is used to avoid collisions
    return "order_" + System.currentTimeMillis() + "_" + Math.abs(random.nextInt(1000));
  }

}
