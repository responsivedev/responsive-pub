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

package dev.responsive.examples.regression;

import static dev.responsive.examples.regression.RegConstants.CUSTOMERS;
import static dev.responsive.examples.regression.RegConstants.ORDERS;
import static dev.responsive.examples.regression.RegConstants.resultsTopic;

import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.RateLimiter;
import dev.responsive.examples.common.E2ETestUtils;
import dev.responsive.examples.e2etest.UrandomGenerator;
import dev.responsive.examples.regression.gen.CustomerGen;
import dev.responsive.examples.regression.gen.OrderGen;
import dev.responsive.examples.regression.model.Customer;
import dev.responsive.examples.regression.model.Order;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OrderAndCustomerDriver extends AbstractExecutionThreadService {

  private static final Logger LOG = LoggerFactory.getLogger(OrderAndCustomerDriver.class);
  private static final double RECORDS_PER_SECOND = 100.0;

  private final UrandomGenerator random = new UrandomGenerator();
  private final Map<String, Object> props;

  private final KafkaProducer<String, Order> orderProducer;
  private final KafkaProducer<String, Customer> customerProducer;
  private final RateLimiter rateLimiter = RateLimiter.create(RECORDS_PER_SECOND);
  private final CustomerGen customerGen = new CustomerGen(random);
  private final OrderGen orderGen = new OrderGen(random, customerGen);

  public OrderAndCustomerDriver(final Map<String, Object> props) {
    this.props = new HashMap<>(props);
    this.orderProducer = getProducer(props, RegressionSchema.OrderSerializer.class);
    this.customerProducer = getProducer(props, RegressionSchema.CustomerSerializer.class);
  }

  private static <V> KafkaProducer<String, V> getProducer(
      final Map<String, Object> original,
      final Class<? extends Serializer<V>> serializerClass
  ) {
    final var props = new HashMap<>(original);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, serializerClass);
    props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
    props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 60_000); // 1 minute
    props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 240_000); // 4 minutes

    return new KafkaProducer<>(props);
  }

  @Override
  protected void startUp() {
    LOG.info("Starting OrderAndCustomerDriver...");
    E2ETestUtils.maybeCreateTopics(
        props,
        RegConstants.NUM_PARTITIONS,
        List.of(ORDERS, CUSTOMERS, resultsTopic(true), resultsTopic(false))
    );
    LOG.info("Created topics.");
  }

  @Override
  protected void shutDown() {
  }

  @Override
  protected void run() throws ExecutionException, InterruptedException {
    LOG.info("Running OrderAndCustomerDriver...");
    // create the first customer so that orders will have a valid customer id
    customerProducer.send(newCustomer()).get();

    int orders = 0;
    int customers = 1;

    while (isRunning()) {
      rateLimiter.acquire();

      final boolean isOrder = random.nextByte() % 8 != 0; // 8:1 ratio of orders to customers
      if (isOrder) {
        orders++;
        orderProducer.send(newOrder()).get();
      } else {
        customers++;
        customerProducer.send(newCustomer()).get();
      }

      if ((orders + customers) % 1000 == 0) {
        LOG.info("Produced {} orders and {} customers", orders, customers);
      }
    }
  }

  private ProducerRecord<String, Customer> newCustomer() {
    final Customer customer = customerGen.next();
    final boolean isTombstone = random.nextByte() % 5 == 0; // 5% chance of a tombstone

    return new ProducerRecord<>(
        CUSTOMERS,
        customer.customerId(),
        isTombstone ? null : customer
    );
  }

  private ProducerRecord<String, Order> newOrder() {
    // key on customer id so that the order can be joined with the customer
    // without a repartition (which would introduce indeterminate results)
    final Order order = orderGen.next();
    return new ProducerRecord<>(
        ORDERS,
        order.customerId(),
        order
    );
  }
}
