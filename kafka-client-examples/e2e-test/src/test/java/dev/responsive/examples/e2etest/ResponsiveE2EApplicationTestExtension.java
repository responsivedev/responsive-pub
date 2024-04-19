package dev.responsive.examples.e2etest;

import static dev.responsive.kafka.api.config.ResponsiveConfig.CASSANDRA_CHECK_INTERVAL_MS;
import static dev.responsive.kafka.api.config.ResponsiveConfig.CASSANDRA_DATACENTER_CONFIG;
import static dev.responsive.kafka.api.config.ResponsiveConfig.CASSANDRA_DESIRED_NUM_PARTITION_CONFIG;
import static dev.responsive.kafka.api.config.ResponsiveConfig.CASSANDRA_HOSTNAME_CONFIG;
import static dev.responsive.kafka.api.config.ResponsiveConfig.CASSANDRA_PORT_CONFIG;
import static dev.responsive.kafka.api.config.ResponsiveConfig.RESPONSIVE_ENV_CONFIG;
import static dev.responsive.kafka.api.config.ResponsiveConfig.RESPONSIVE_ORG_CONFIG;
import static dev.responsive.kafka.api.config.ResponsiveConfig.STORAGE_BACKEND_TYPE_CONFIG;
import static dev.responsive.kafka.api.config.ResponsiveConfig.TASK_ASSIGNOR_CLASS_OVERRIDE;
import static dev.responsive.kafka.api.config.ResponsiveConfig.loggedConfig;
import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.InternalConfig.INTERNAL_TASK_ASSIGNOR_CLASS;

import dev.responsive.kafka.api.config.ResponsiveConfig;
import dev.responsive.kafka.api.config.StorageBackend;
import java.lang.reflect.Parameter;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.admin.Admin;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

public class ResponsiveE2EApplicationTestExtension implements BeforeAllCallback, AfterAllCallback,
    ParameterResolver {

  private static final DockerImageName CASSANDRA = DockerImageName.parse("cassandra:4.1.0");
  private static final DockerImageName KAFKA = DockerImageName.parse("confluentinc/cp-kafka:7.3.2");

  public static CassandraContainer<?> cassandra = new CassandraContainer<>(CASSANDRA)
      .withInitScript("CassandraDockerInit.cql")
      .withReuse(true);
  public static KafkaContainer kafka = new KafkaContainer(KAFKA)
      .withEnv("KAFKA_GROUP_MIN_SESSION_TIMEOUT_MS", "1000")
      .withEnv("KAFKA_GROUP_MAX_SESSION_TIMEOUT_MS", "60000")
      .withReuse(true);

  public static Admin admin;

  public StorageBackend backend = StorageBackend.CASSANDRA;

  public ResponsiveE2EApplicationTestExtension() {
  }

  public ResponsiveE2EApplicationTestExtension(final StorageBackend backend) {
    this.backend = backend;
  }

  @Override
  public void beforeAll(final ExtensionContext context) throws Exception {
    switch (backend) {
      case CASSANDRA:
        cassandra.start();
        break;
      case MONGO_DB:
      default:
        throw new IllegalStateException("Unexpected value: " + backend);
    }

    kafka.start();
    admin = Admin.create(Map.of(BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()));
  }

  @Override
  public void afterAll(final ExtensionContext context) throws Exception {
    cassandra.stop();
    kafka.stop();
    admin.close();
  }

  @Override
  public boolean supportsParameter(
      final ParameterContext parameterContext,
      final ExtensionContext extensionContext
  ) throws ParameterResolutionException {
    return parameterContext.getParameter().getType().equals(CassandraContainer.class)
        || parameterContext.getParameter().getType().equals(KafkaContainer.class)
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
    } else if (parameterContext.getParameter().getType() == KafkaContainer.class) {
      return kafka;
    } else if (parameterContext.getParameter().getType() == Admin.class) {
      return admin;
    } else if (isContainerConfig(parameterContext)) {
      final Map<String, Object> map = new HashMap<>(Map.of(
          RESPONSIVE_ORG_CONFIG, "responsive",
          RESPONSIVE_ENV_CONFIG, "itests",
          INTERNAL_TASK_ASSIGNOR_CLASS, TASK_ASSIGNOR_CLASS_OVERRIDE,
          BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
          CASSANDRA_DESIRED_NUM_PARTITION_CONFIG, -1,
          CASSANDRA_CHECK_INTERVAL_MS, 100
      ));

      switch (backend) {
        case CASSANDRA:
          map.put(STORAGE_BACKEND_TYPE_CONFIG, StorageBackend.CASSANDRA.name());
          map.put(CASSANDRA_HOSTNAME_CONFIG, cassandra.getContactPoint().getHostName());
          map.put(CASSANDRA_PORT_CONFIG, cassandra.getContactPoint().getPort());
          map.put(CASSANDRA_DATACENTER_CONFIG, cassandra.getLocalDatacenter());
          break;
        default:
          throw new IllegalStateException("Unexpected value: " + backend);
      }

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
}
