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
import static dev.responsive.kafka.api.config.ResponsiveConfig.MONGO_CONNECTION_STRING_CONFIG;
import static dev.responsive.kafka.api.config.ResponsiveConfig.RESPONSIVE_ENV_CONFIG;
import static dev.responsive.kafka.api.config.ResponsiveConfig.RESPONSIVE_LICENSE_CONFIG;
import static dev.responsive.kafka.api.config.ResponsiveConfig.RESPONSIVE_LICENSE_SERVER_CONFIG;
import static dev.responsive.kafka.api.config.ResponsiveConfig.RESPONSIVE_ORG_CONFIG;
import static dev.responsive.kafka.api.config.ResponsiveConfig.RS3_HOSTNAME_CONFIG;
import static dev.responsive.kafka.api.config.ResponsiveConfig.RS3_PORT_CONFIG;
import static dev.responsive.kafka.api.config.ResponsiveConfig.STORAGE_BACKEND_TYPE_CONFIG;
import static dev.responsive.kafka.api.config.ResponsiveConfig.loggedConfig;
import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;

import dev.responsive.kafka.api.config.ResponsiveConfig;
import dev.responsive.kafka.api.config.StorageBackend;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Parameter;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.file.Files;
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
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.MountableFile;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketLocationConstraint;
import software.amazon.awssdk.services.s3.model.CreateBucketConfiguration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

public class ResponsiveExtension
    implements BeforeEachCallback, AfterEachCallback, ParameterResolver {

  private static final Logger LOG = LoggerFactory.getLogger(ResponsiveExtension.class);

  public static final String RS3_ALLOW_S3_HTTP = "RS3_ALLOW_S3_HTTP";

  public static CassandraContainer<?> cassandra = new CassandraContainer<>(TestConstants.CASSANDRA)
      .withInitScript("CassandraDockerInit.cql")
      .withReuse(true);

  private static final Network rs3Network = Network.newNetwork();

  public static MinIOContainer minio
      = new MinIOContainer("minio/minio:RELEASE.2025-03-12T18-04-18Z")
      .withUserName("rs3testkey")
      .withPassword("rs3testsecret")
      .withNetwork(rs3Network)
      .withNetworkAliases("minio");

  public static RS3Container rs3 = new RS3Container("rs3:test-snapshots")
      .withCopyFileToContainer(
          MountableFile.forClasspathResource("/rs3config.toml"),
          "/config.toml"
      )
      .withEnv("AWS_ACCESS_KEY_ID", "rs3testkey")
      .withEnv("AWS_SECRET_ACCESS_KEY", "rs3testsecret")
      .withEnv("CONFIG_FILE", "/config.toml")
      .withEnv(RS3_ALLOW_S3_HTTP, "true")
      .withNetwork(rs3Network)
      .withExposedPorts(50051);

  public static KafkaContainer kafka = new KafkaContainer(TestConstants.KAFKA)
      .withEnv("KAFKA_GROUP_MIN_SESSION_TIMEOUT_MS", "1000")
      .withEnv("KAFKA_GROUP_MAX_SESSION_TIMEOUT_MS", "60000")
      .withReuse(true);
  public static MongoDBContainer mongo = new MongoDBContainer(TestConstants.MONGODB);
  public static TestLicenseServer licenseServer = new TestLicenseServer();

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
    final var storageFuture = Startables.deepStart(cassandra, mongo);
    final var minioFuture = Startables.deepStart(minio);
    try {
      licenseServer.start();
      kafkaFuture.get();
      admin = Admin.create(Map.of(BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()));
      storageFuture.get();
      minioFuture.get();
      initRS3Bucket(minio);
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
    kafka.stop();
    admin.close();
    licenseServer.close();
    minio.stop();
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
        || parameterContext.getParameter().getType().equals(TestLicenseServer.class)
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
    } else if (parameterContext.getParameter().getType() == Admin.class) {
      return admin;
    } else if (parameterContext.getParameter().getType() == TestLicenseServer.class) {
      return licenseServer;
    } else if (isContainerConfig(parameterContext)) {
      final Map<String, Object> map = new HashMap<>(Map.of(
          RESPONSIVE_ORG_CONFIG, "responsive",
          RESPONSIVE_ENV_CONFIG, "itests",
          BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
          CASSANDRA_DESIRED_NUM_PARTITION_CONFIG, -1,
          CASSANDRA_CHECK_INTERVAL_MS, 100,
          RESPONSIVE_LICENSE_CONFIG, LicenseUtils.getUsageLicense(),
          RESPONSIVE_LICENSE_SERVER_CONFIG, "http://localhost:" + licenseServer.port()
      ));

      if (backend == StorageBackend.CASSANDRA) {
        map.put(STORAGE_BACKEND_TYPE_CONFIG, StorageBackend.CASSANDRA.name());
      } else if (backend == StorageBackend.MONGO_DB) {
        map.put(STORAGE_BACKEND_TYPE_CONFIG, StorageBackend.MONGO_DB.name());
      } else if (backend == StorageBackend.RS3) {
        map.put(STORAGE_BACKEND_TYPE_CONFIG, StorageBackend.RS3.name());
        map.put(RS3_HOSTNAME_CONFIG, "localhost");
        map.put(RS3_PORT_CONFIG, rs3.getMappedPort(50051));
      }

      // throw in configuration for both backends since som tests are parameterized with both
      // cassandra and mongo
      map.put(CASSANDRA_HOSTNAME_CONFIG, cassandra.getContactPoint().getHostName());
      map.put(CASSANDRA_PORT_CONFIG, cassandra.getContactPoint().getPort());
      map.put(CASSANDRA_DATACENTER_CONFIG, cassandra.getLocalDatacenter());

      map.put(MONGO_CONNECTION_STRING_CONFIG, mongo.getConnectionString());

      if (parameterContext.getParameter().getType().equals(Map.class)) {
        return map;
      } else  {
        return loggedConfig(map);
      }
    }

    throw new IllegalArgumentException("Unexpected parameter " + parameterContext);
  }

  private static void initRS3Bucket(final MinIOContainer minio) {
    try (final S3Client s3 = S3Client.builder()
        .endpointOverride(URI.create(minio.getS3URL()))
        .credentialsProvider(
            StaticCredentialsProvider.create(
                AwsBasicCredentials.create("rs3testkey", "rs3testsecret")))
        .region(Region.US_WEST_2)
        .forcePathStyle(true)
        .build()) {
      s3.createBucket(CreateBucketRequest.builder()
          .bucket("rs3-test")
          .createBucketConfiguration(CreateBucketConfiguration.builder()
              .locationConstraint(BucketLocationConstraint.US_WEST_2)
              .build())
          .build());
    }
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
