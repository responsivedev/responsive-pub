package dev.responsive.examples.rs3.demo;

import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.RateLimiter;
import dev.responsive.examples.common.E2ETestUtils;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OrderDriver extends AbstractExecutionThreadService {

  public static final String ORDERS = "orders";

  private static final Logger LOG = LoggerFactory.getLogger(OrderDriver.class);

  private final Random random = new Random(0);
  private final Map<String, Object> props;

  private final KafkaProducer<String, Order> orderProducer;
  private final RateLimiter rateLimiter = RateLimiter.create(Constants.RECORDS_PER_SECOND);
  private final OrderGen orderGen;

  public OrderDriver(
      final Map<String, Object> props,
      final int numCustomers
  ) {
    this.props = new HashMap<>(props);
    this.orderProducer = getProducer(props, Schema.OrderSerializer.class);
    this.orderGen = new OrderGen(random, numCustomers, 50);
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
        8,
        List.of(ORDERS)
    );
    LOG.info("Created topics.");
  }

  @Override
  protected void shutDown() {
  }

  @Override
  protected void run() throws ExecutionException, InterruptedException {
    try {
      doRun();
    } catch (final Exception e) {
      LOG.error("failed to run driver", e);
      throw e;
    }
  }

  void doRun() throws ExecutionException, InterruptedException {
    LOG.info("Running OrderAndCustomerDriver...");
    int orders = 0;

    while (isRunning()) {
      rateLimiter.acquire();

      orders++;
      orderProducer.send(newOrder()).get();

      if (orders % 1000 == 0) {
        LOG.info("Produced {} orders and {} customers", orders);
      }
    }
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