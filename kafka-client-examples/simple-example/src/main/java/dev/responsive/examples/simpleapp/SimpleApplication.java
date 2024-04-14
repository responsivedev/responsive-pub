package dev.responsive.examples.simpleapp;

import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.REQUEST_TIMEOUT;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.CreateKeyspace;
import dev.responsive.kafka.api.ResponsiveKafkaStreams;
import dev.responsive.kafka.api.stores.ResponsiveKeyValueParams;
import dev.responsive.kafka.api.stores.ResponsiveStores;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleApplication {
  private static final Logger LOG = LoggerFactory.getLogger(SimpleApplication.class);

  public static final class Config extends AbstractConfig {
    private static final String PREFIX = "responsive.simple.app.";
    private static final String SOURCE = PREFIX + "source";
    private static final String NAME = PREFIX + "name";
    private static final ConfigDef CONFIG_DEF = new ConfigDef()
        .define(NAME, ConfigDef.Type.STRING, "", Importance.LOW, "test run name")
        .define(SOURCE, ConfigDef.Type.STRING, "", ConfigDef.Importance.LOW, "source topic");

    private Config(final Map<?, ?> properties) {
      super(CONFIG_DEF, properties);
    }

    private String getSourceTopic() {
      return getString(NAME) + "-" + getString(SOURCE);
    }
  }

  private final KafkaStreams kafkaStreams;
  private final Config config;
  private boolean stopped = false;

  public SimpleApplication(final Map<?, ?> properties) {
    config = new Config(properties);
    maybeCreateTopics();
    maybeCreateKeyspace();
    LOG.info("build topology");
    kafkaStreams = buildTopology(config, properties);
  }

  public synchronized void start() {
    if (!stopped) {
      LOG.info("start kafka streams");
      kafkaStreams.start();
    }
  }

  public synchronized void stop() {
    if (!stopped) {
      kafkaStreams.close();
      stopped = true;
      this.notify();
    }
  }

  public synchronized void await() {
    while (!stopped) {
      try {
        this.wait();
      } catch (final InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private static KafkaStreams buildTopology(final Config cfg, Map<?, ?> rawCfg) {
    final StreamsBuilder builder = new StreamsBuilder();
    final String source = cfg.getSourceTopic();
    final KStream<byte[], byte[]> stream =
        builder.stream(source, Consumed.with(Serdes.ByteArray(), Serdes.ByteArray()));
    final AtomicLong msgCounter = new AtomicLong(0);
    final KStream<byte[], byte[]> result = stream.mapValues((k, v) -> {
      final var msgCount = msgCounter.incrementAndGet();
      if (msgCount % 1000 == 0) {
        System.out.printf("received %d %s%n", msgCount, new String(v));
      }
      return v;
    });
    final KTable<byte[], Long> counts = stream.groupByKey()
        .count(ResponsiveStores.materialized(
            ResponsiveKeyValueParams.keyValue(cfg.getString(Config.NAME))));
    result.to(source + "-out", Produced.with(Serdes.ByteArray(), Serdes.ByteArray()));
    counts.toStream().to(source + "-counts", Produced.with(Serdes.ByteArray(), Serdes.Long()));
    final Properties properties = new Properties();
    properties.putAll(rawCfg);
    return new ResponsiveKafkaStreams(builder.build(properties), properties);
  }

  private void maybeCreateTopics() {
    try (final Admin admin = Admin.create(config.originals())) {
      final String source = config.getSourceTopic();
      for (final var topic : List.of(source, source + "-out", source + "-counts")) {
        LOG.info("create topic {}", topic);
        try {
          admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1)));
        } catch (final RuntimeException e) {
          LOG.info("Error creating topic: " + e);
        }
      }
    }
  }

  private void maybeCreateKeyspace() {
    LOG.info("create keyspace responsive_test");
    try (final CqlSession session = cqlSession()) {
      final CreateKeyspace createKeyspace = SchemaBuilder.createKeyspace("responsive_test")
          .ifNotExists()
          .withSimpleStrategy(1);
      session.execute(createKeyspace.build());
    }
  }

  private CqlSession cqlSession() {
    return CqlSession.builder()
        .addContactPoint(new InetSocketAddress("scylla-svc", 9042))
        .withLocalDatacenter("datacenter1")
        .withConfigLoader(DriverConfigLoader.programmaticBuilder()
            .withLong(REQUEST_TIMEOUT, 10000)
            .build())
        .build();
  }
}
