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

package dev.responsive.kafka.testutils;

import static dev.responsive.kafka.api.config.ResponsiveConfig.CASSANDRA_CHECK_INTERVAL_MS;
import static dev.responsive.kafka.api.config.ResponsiveConfig.CASSANDRA_DATACENTER_CONFIG;
import static dev.responsive.kafka.api.config.ResponsiveConfig.CASSANDRA_DESIRED_NUM_PARTITION_CONFIG;
import static dev.responsive.kafka.api.config.ResponsiveConfig.CASSANDRA_HOSTNAME_CONFIG;
import static dev.responsive.kafka.api.config.ResponsiveConfig.CASSANDRA_PORT_CONFIG;
import static dev.responsive.kafka.api.config.ResponsiveConfig.DYNAMODB_ENDPOINT_CONFIG;
import static dev.responsive.kafka.api.config.ResponsiveConfig.MONGO_ENDPOINT_CONFIG;
import static dev.responsive.kafka.api.config.ResponsiveConfig.RESPONSIVE_ENV_CONFIG;
import static dev.responsive.kafka.api.config.ResponsiveConfig.RESPONSIVE_LICENSE_CONFIG;
import static dev.responsive.kafka.api.config.ResponsiveConfig.RESPONSIVE_ORG_CONFIG;
import static dev.responsive.kafka.api.config.ResponsiveConfig.RS3_HOSTNAME_CONFIG;
import static dev.responsive.kafka.api.config.ResponsiveConfig.RS3_PORT_CONFIG;
import static dev.responsive.kafka.api.config.ResponsiveConfig.STORAGE_BACKEND_TYPE_CONFIG;
import static dev.responsive.kafka.api.config.ResponsiveConfig.TASK_ASSIGNOR_CLASS_OVERRIDE;
import static dev.responsive.kafka.api.config.ResponsiveConfig.loggedConfig;
import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.InternalConfig.INTERNAL_TASK_ASSIGNOR_CLASS;

import dev.responsive.kafka.api.config.ResponsiveConfig;
import dev.responsive.kafka.api.config.StorageBackend;
import java.lang.reflect.Parameter;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.admin.Admin;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.lifecycle.Startables;

public class ResponsiveExtension
    implements BeforeEachCallback, AfterEachCallback, ParameterResolver {

  private static final Logger LOG = LoggerFactory.getLogger(ResponsiveExtension.class);

  public static CassandraContainer<?> cassandra = new CassandraContainer<>(TestConstants.CASSANDRA)
      .withInitScript("CassandraDockerInit.cql")
      .withReuse(true);
  public static RS3Container rs3
      = new RS3Container("public.ecr.aws/j8q9y0n6/responsiveinc/rs3-server:latest")
      .withExposedPorts(50051);
  public static KafkaContainer kafka = new KafkaContainer(TestConstants.KAFKA)
      .withEnv("KAFKA_GROUP_MIN_SESSION_TIMEOUT_MS", "1000")
      .withEnv("KAFKA_GROUP_MAX_SESSION_TIMEOUT_MS", "60000")
      .withReuse(true);
  public static MongoDBContainer mongo = new MongoDBContainer(TestConstants.MONGODB);
  public static DynamoDbContainer dynamo = new DynamoDbContainer(TestConstants.DYNAMODB);

  public static Admin admin;

  public StorageBackend backend = StorageBackend.MONGO_DB;

  static {
    startAll();

    Runtime.getRuntime().addShutdownHook(new Thread(ResponsiveExtension::stopAll));
  }

  @Override
  public void beforeEach(ExtensionContext extensionContext) {
    if (backend.equals(StorageBackend.RS3)) {
      rs3.stop();
      rs3.start();
    }
  }

  @Override
  public void afterEach(ExtensionContext extensionContext) {
    if (backend.equals(StorageBackend.RS3)) {
      rs3.stop();
    }
  }

  public static void startAll() {
    final Instant start = Instant.now();
    LOG.info("Starting up Responsive test harness at {}", start);

    final var kafkaFuture = Startables.deepStart(kafka);
    final var storageFuture = Startables.deepStart(cassandra, mongo, dynamo);
    try {
      kafkaFuture.get();
      admin = Admin.create(Map.of(BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()));
      storageFuture.get();
    } catch (final Exception e) {
      throw new RuntimeException("Responsive test harness startup failed", e);
    }

    final Instant end = Instant.now();
    LOG.info("Finished starting up Responsive test harness at {}, took {}",
             end, Duration.ofMillis(end.toEpochMilli() - start.toEpochMilli()));
  }

  public static void stopAll() {
    cassandra.stop();
    mongo.stop();
    dynamo.stop();
    kafka.stop();
    admin.close();
  }

  public ResponsiveExtension() {
  }

  public ResponsiveExtension(final StorageBackend backend) {
    this.backend = backend;
  }

  @Override
  public boolean supportsParameter(
      final ParameterContext parameterContext,
      final ExtensionContext extensionContext
  ) throws ParameterResolutionException {
    return parameterContext.getParameter().getType().equals(CassandraContainer.class)
        || parameterContext.getParameter().getType().equals(KafkaContainer.class)
        || parameterContext.getParameter().getType().equals(MongoDBContainer.class)
        || parameterContext.getParameter().getType().equals(RS3Container.class)
        || parameterContext.getParameter().getType().equals(Admin.class)
        || isContainerConfig(parameterContext);
  }

  @Override
  public Object resolveParameter(
      final ParameterContext parameterContext,
      final ExtensionContext extensionContext
  ) throws ParameterResolutionException {
    if (parameterContext.getParameter().getType() == CassandraContainer.class) {
      return cassandra;
    } else if (parameterContext.getParameter().getType() == MongoDBContainer.class) {
      return mongo;
    } else if (parameterContext.getParameter().getType() == KafkaContainer.class) {
      return kafka;
    } else if (parameterContext.getParameter().getType() == RS3Container.class) {
      return rs3;
    } else if (parameterContext.getParameter().getType() == DynamoDbContainer.class) {
      return dynamo;
    } else if (parameterContext.getParameter().getType() == Admin.class) {
      return admin;
    } else if (isContainerConfig(parameterContext)) {
      final Map<String, Object> map = new HashMap<>(Map.of(
          RESPONSIVE_ORG_CONFIG, "responsive",
          RESPONSIVE_ENV_CONFIG, "itests",
          INTERNAL_TASK_ASSIGNOR_CLASS, TASK_ASSIGNOR_CLASS_OVERRIDE,
          BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
          CASSANDRA_DESIRED_NUM_PARTITION_CONFIG, -1,
          CASSANDRA_CHECK_INTERVAL_MS, 100,
          RESPONSIVE_LICENSE_CONFIG, LicenseUtils.getLicense()
      ));

      if (backend == StorageBackend.CASSANDRA) {
        map.put(STORAGE_BACKEND_TYPE_CONFIG, StorageBackend.CASSANDRA.name());
      } else if (backend == StorageBackend.MONGO_DB) {
        map.put(STORAGE_BACKEND_TYPE_CONFIG, StorageBackend.MONGO_DB.name());
      } else if (backend == StorageBackend.RS3) {
        map.put(STORAGE_BACKEND_TYPE_CONFIG, StorageBackend.RS3.name());
        map.put(RS3_HOSTNAME_CONFIG, "localhost");
        map.put(RS3_PORT_CONFIG, rs3.getMappedPort(50051));
      } else if (backend == StorageBackend.DYNAMO_DB) {
        map.put(STORAGE_BACKEND_TYPE_CONFIG, StorageBackend.DYNAMO_DB.name());
      }

      // throw in configuration for both backends since som tests are parameterized with both
      // cassandra and mongo
      map.put(CASSANDRA_HOSTNAME_CONFIG, cassandra.getContactPoint().getHostName());
      map.put(CASSANDRA_PORT_CONFIG, cassandra.getContactPoint().getPort());
      map.put(CASSANDRA_DATACENTER_CONFIG, cassandra.getLocalDatacenter());
      map.put(MONGO_ENDPOINT_CONFIG, mongo.getConnectionString());
      map.put(DYNAMODB_ENDPOINT_CONFIG, dynamo.getConnectionString());

      if (parameterContext.getParameter().getType().equals(Map.class)) {
        return map;
      } else  {
        return loggedConfig(map);
      }
    }

    throw new IllegalArgumentException("Unexpected parameter " + parameterContext);
  }

  private static boolean isContainerConfig(final ParameterContext context) {
    final Parameter param = context.getParameter();
    return (param.getType().equals(Map.class) || param.getType().equals(ResponsiveConfig.class))
        && param.getAnnotation(ResponsiveConfigParam.class) != null;
  }

  public static class RS3Container extends GenericContainer<RS3Container> {
    public RS3Container(final String image) {
      super(image);
    }
  }
}
