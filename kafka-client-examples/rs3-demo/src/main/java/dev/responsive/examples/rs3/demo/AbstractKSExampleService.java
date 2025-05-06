package dev.responsive.examples.rs3.demo;

import com.google.common.util.concurrent.AbstractIdleService;
import dev.responsive.examples.common.E2ETestUtils;
import dev.responsive.kafka.api.ResponsiveKafkaStreams;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractKSExampleService extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractKSExampleService.class);

  private final Executor executor = Executors.newSingleThreadExecutor();

  private final Map<String, Object> properties;
  private final String name;

  private KafkaStreams kafkaStreams;

  public AbstractKSExampleService(
      final String name,
      final Map<String, Object> props
  ) {
    this.name = name;
    this.properties = E2ETestUtils.defaultStreamProps(props);
  }

  @Override
  protected final Executor executor() {
    // override the default executor to ensure that startUp() and shutDown() run on
    // the same thread (i.e. ensuring that the service completes startUp before calling shutDown)
    return executor;
  }

  @Override
  protected final void startUp() throws Exception {
    LOG.info("Starting {}...", name);
    E2ETestUtils.maybeCreateTopics(
        properties,
        Constants.NUM_PARTITIONS,
        List.of(
            Constants.ORDERS,
            Constants.SUMMARIZED_ORDERS_TOPIC
        )
    );
    LOG.info("Created topics...");

    Topology topology = buildTopology();
    LOG.info("Starting Kafka Streams with topology \n{}", topology.describe());

    kafkaStreams = new ResponsiveKafkaStreams(buildTopology(), properties);
    kafkaStreams.setUncaughtExceptionHandler(
        e -> {
          LOG.error("uncaught error on stream thread", e);
          return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
        });
    kafkaStreams.start();
    LOG.info("Kafka Streams started!");
  }

  @Override
  protected final void shutDown() throws Exception {
    if (kafkaStreams != null) {
      kafkaStreams.close();
    }
  }

  protected abstract Topology buildTopology();

  protected final String resultsTopic() {
    return Constants.SUMMARIZED_ORDERS_TOPIC;
  }

  public String name() {
    return name;
  }
}
