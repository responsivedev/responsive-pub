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

import static dev.responsive.examples.regression.RegConstants.CUSTOMER_ID_TO_NAME;
import static dev.responsive.examples.regression.RegConstants.CUSTOMER_NAME_TO_LOCATION;
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
  private final KafkaProducer<String, String> customerProducer;
  private final RateLimiter rateLimiter = RateLimiter.create(RECORDS_PER_SECOND);
  private final CustomerGen customerGen = new CustomerGen(random);
  private final OrderGen orderGen = new OrderGen(random, customerGen);

  public OrderAndCustomerDriver(final Map<String, Object> props) {
    this.props = new HashMap<>(props);
    this.orderProducer = getProducer(props, RegressionSchema.OrderSerializer.class);
    this.customerProducer = getProducer(props, StringSerializer.class);
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
        List.of(ORDERS,
                CUSTOMER_NAME_TO_LOCATION,
                CUSTOMER_ID_TO_NAME,
                resultsTopic(true),
                resultsTopic(false)
        )
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
    sendNewCustomer();

    int orders = 0;
    int customers = 1;

    while (isRunning()) {
      rateLimiter.acquire();

      final boolean isOrder = random.nextByte() % 8 != 0; // 8:1 ratio of orders to customers
      if (isOrder) {
        orders++;
        sendNewOrder();
      } else {
        customers++;
        sendNewCustomer();
      }

      if ((orders + customers) % 1000 == 0) {
        LOG.info("Produced {} orders and {} customers", orders, customers);
      }
    }
  }

  // 1 customer is actually two records/topics -- one for name-id info and one for id-location
  // we send both at the same time and with the same timestamp to ensure these get joined
  private void sendNewCustomer() throws ExecutionException, InterruptedException {

    final Customer customer = customerGen.next();
    final long timestamp = System.currentTimeMillis();

    final boolean isTombstone = random.nextByte() % 5 == 0; // 5% chance of a tombstone

    final var idToNameRecord = new ProducerRecord<>(
        CUSTOMER_ID_TO_NAME,
        null,
        timestamp,
        customer.customerId(),
        isTombstone ? null : customer.customerName()
    );
    final var nameToLocationRecord = new ProducerRecord<>(
        CUSTOMER_NAME_TO_LOCATION,
        null,
        timestamp,
        customer.customerName(),
        isTombstone ? null : customer.location()
    );

    customerProducer.send(idToNameRecord).get();
    customerProducer.send(nameToLocationRecord).get();
  }

  private void sendNewOrder() throws ExecutionException, InterruptedException {
    // key on customer id so that the order can be joined with the customer
    // without a repartition (which would introduce indeterminate results)
    final Order order = orderGen.next();
    orderProducer.send(
        new ProducerRecord<>(
            ORDERS,
            null,
            System.currentTimeMillis(),
            order.customerId(),
            order
        )
    ).get();
  }
}
