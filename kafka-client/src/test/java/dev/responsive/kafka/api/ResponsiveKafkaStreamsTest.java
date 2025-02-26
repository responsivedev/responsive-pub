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

package dev.responsive.kafka.api;

import static dev.responsive.kafka.api.config.ResponsiveConfig.COMPATIBILITY_MODE_CONFIG;
import static dev.responsive.kafka.api.config.ResponsiveConfig.PLATFORM_API_KEY_CONFIG;
import static dev.responsive.kafka.api.config.ResponsiveConfig.RESPONSIVE_ENV_CONFIG;
import static dev.responsive.kafka.api.config.ResponsiveConfig.RESPONSIVE_LICENSE_CONFIG;
import static dev.responsive.kafka.api.config.ResponsiveConfig.RESPONSIVE_LICENSE_FILE_CONFIG;
import static dev.responsive.kafka.api.config.ResponsiveConfig.RESPONSIVE_ORG_CONFIG;
import static dev.responsive.kafka.api.config.ResponsiveConfig.STORAGE_BACKEND_TYPE_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DSL_STORE_SUPPLIERS_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlSession;
import dev.responsive.kafka.api.config.CompatibilityMode;
import dev.responsive.kafka.api.config.ResponsiveConfig;
import dev.responsive.kafka.api.config.StorageBackend;
import dev.responsive.kafka.api.stores.ResponsiveDslStoreSuppliers;
import dev.responsive.kafka.internal.db.CassandraClient;
import dev.responsive.kafka.internal.db.CassandraClientFactory;
import dev.responsive.kafka.internal.license.exception.LicenseAuthenticationException;
import dev.responsive.kafka.internal.license.exception.LicenseUseViolationException;
import dev.responsive.kafka.internal.metrics.ResponsiveMetrics;
import dev.responsive.kafka.testutils.IntegrationTestUtils;
import dev.responsive.kafka.testutils.LicenseUtils;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ResponsiveKafkaStreamsTest {
  private static final String DECODED_INVALID_LICENSE_FILE
      = "test-licenses/test-license-invalid-signature.json";
  private static final String DECODED_TRIAL_EXPIRED_LICENSE_FILE
      = "test-licenses/test-license-trial-expired.json";

  private final KafkaClientSupplier supplier = new DefaultKafkaClientSupplier() {
    @Override
    public Admin getAdmin(final Map<String, Object> config) {
      return admin;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Producer<byte[], byte[]> getProducer(final Map<String, Object> config) {
      return Mockito.mock(Producer.class);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Consumer<byte[], byte[]> getConsumer(final Map<String, Object> config) {
      final var mock = Mockito.mock(Consumer.class);
      when(mock.groupMetadata()).thenReturn(new ConsumerGroupMetadata("group.id"));
      return mock;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Consumer<byte[], byte[]> getRestoreConsumer(final Map<String, Object> config) {
      return Mockito.mock(Consumer.class);
    }
  };

  private final CassandraClientFactory mockCassandraFactory = new CassandraClientFactory() {
    @Override
    public CqlSession createCqlSession(
        final ResponsiveConfig config,
        final ResponsiveMetrics metrics
    ) {
      return Mockito.mock(CqlSession.class);
    }

    @Override
    public CassandraClient createClient(final CqlSession session, final ResponsiveConfig config) {
      return Mockito.mock(CassandraClient.class);
    }
  };

  @Mock
  private Admin admin;
  private Map<String, Object> properties;

  @BeforeEach
  public void setUp() {
    properties = new HashMap<>();
    properties.putAll(IntegrationTestUtils.dummyConfig().originals());
    properties.put(BOOTSTRAP_SERVERS_CONFIG, "foo:8082");

    properties.put(KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
    properties.put(VALUE_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
    properties.put(KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
    properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);

    properties.put(APPLICATION_ID_CONFIG, "kafka-streams-test");
    properties.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.LongSerde.class.getName());
    properties.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.LongSerde.class.getName());

    properties.put(RESPONSIVE_ORG_CONFIG, "responsive");
    properties.put(RESPONSIVE_ENV_CONFIG, "license-test");

    properties.put(
        RESPONSIVE_LICENSE_CONFIG,
        LicenseUtils.getTrialLicense()
    );
  }

  @SuppressWarnings("resource")
  @Test
  public void shouldInvalidateBadConfigs() {
    // Given:
    properties.put(NUM_STANDBY_REPLICAS_CONFIG, 2); // a config that would cause failure

    // When:
    final StreamsBuilder builder = new StreamsBuilder();
    builder.stream("foo").to("bar");

    // Then:
    final ConfigException e = assertThrows(
        ConfigException.class,
        () -> new IntegrationTestUtils.MockResponsiveKafkaStreams(
            builder.build(),
            properties,
            supplier,
            mockCassandraFactory
        )
    );
    assertThat(
        e.getMessage(),
        Matchers.containsString("Invalid Streams configuration value for 'num.standby.replicas'")
    );
  }

  @Test
  public void shouldCreateResponsiveKafkaStreamsInNonResponsiveStorageModeWithUnverifiedConfigs() {
    // Given:
    properties.put(STORAGE_BACKEND_TYPE_CONFIG, StorageBackend.NONE.name());
    properties.put(NUM_STANDBY_REPLICAS_CONFIG, 2); // a config that would cause failure

    // When:
    final StreamsBuilder builder = new StreamsBuilder();
    builder.stream("foo").to("bar");

    final var ks = new ResponsiveKafkaStreams(builder.build(), properties, supplier);

    // Then:
    // no error is thrown
    ks.close();
  }

  @SuppressWarnings("deprecation")
  @Test
  public void shouldCreateResponsiveKafkaStreamsInMetricsCompatibilityModeWithUnverifiedConfigs() {
    // Given:
    properties.put(COMPATIBILITY_MODE_CONFIG, CompatibilityMode.METRICS_ONLY.name());
    properties.put(NUM_STANDBY_REPLICAS_CONFIG, 2); // a config that would cause failure

    // When:
    final StreamsBuilder builder = new StreamsBuilder();
    builder.stream("foo").to("bar");

    final var ks = new ResponsiveKafkaStreams(builder.build(), properties, supplier);

    // Then:
    // no error is thrown
    ks.close();
  }

  @Test
  public void shouldThrowWhenUsingResponsiveDslStoreSuppliersInNonResponsiveStorageMode() {
    // Given:
    properties.put(STORAGE_BACKEND_TYPE_CONFIG, StorageBackend.NONE.name());
    properties.put(DSL_STORE_SUPPLIERS_CLASS_CONFIG, ResponsiveDslStoreSuppliers.class.getName());

    // When:
    final StreamsBuilder builder = new StreamsBuilder();
    builder.stream("foo").to("bar");

    // then:
    final ConfigException e = assertThrows(
        ConfigException.class,
        () -> {
          final var ks = new ResponsiveKafkaStreams(builder.build(), properties, supplier);
          ks.close();
        }
    );
    assertThat(
        e.getMessage(),
        Matchers.containsString(
            "Invalid Streams configuration value for 'dsl.store.suppliers.class': "
                + "got class dev.responsive.kafka.api.stores.ResponsiveDslStoreSuppliers,"
                + " incompatible with setting 'responsive.storage.backend.type' to NONE"
        )
    );
  }

  @Test
  public void shouldAcceptLicenseInLicenseFile() {
    // given:
    final File licenseFile = writeLicenseFile(LicenseUtils.getTrialLicense());
    properties.put(RESPONSIVE_LICENSE_CONFIG, "");
    properties.put(ResponsiveConfig.RESPONSIVE_LICENSE_FILE_CONFIG, licenseFile.getAbsolutePath());
    properties.put(STORAGE_BACKEND_TYPE_CONFIG, StorageBackend.NONE.name());
    final StreamsBuilder builder = new StreamsBuilder();
    builder.stream("foo").to("bar");

    // when/then (no throw):
    final var ks = new ResponsiveKafkaStreams(builder.build(), properties, supplier);
    ks.close();
  }

  @Test
  public void shouldThrowOnLicenseWithInvalidSignature() {
    // given:
    properties.put(
        RESPONSIVE_LICENSE_CONFIG,
        LicenseUtils.getEncodedLicense(DECODED_INVALID_LICENSE_FILE)
    );
    properties.put(STORAGE_BACKEND_TYPE_CONFIG, StorageBackend.NONE.name());
    final StreamsBuilder builder = new StreamsBuilder();
    builder.stream("foo").to("bar");

    // when/then:
    assertThrows(
        LicenseAuthenticationException.class,
        () -> {
          final var ks = new ResponsiveKafkaStreams(builder.build(), properties, supplier);
          ks.close();
        }
    );
  }

  @Test
  public void shouldThrowOnExpiredLicense() {
    // given:
    properties.put(
        RESPONSIVE_LICENSE_CONFIG,
        LicenseUtils.getEncodedLicense(DECODED_TRIAL_EXPIRED_LICENSE_FILE)
    );
    properties.put(STORAGE_BACKEND_TYPE_CONFIG, StorageBackend.NONE.name());
    final StreamsBuilder builder = new StreamsBuilder();
    builder.stream("foo").to("bar");

    // when/then:
    assertThrows(
        LicenseUseViolationException.class,
        () -> {
          final var ks = new ResponsiveKafkaStreams(builder.build(), properties, supplier);
          ks.close();
        }
    );
  }

  @Test
  public void shouldThrowIfNoLicenseOrApiKeyConfigured() {
    // given:
    properties.put(PLATFORM_API_KEY_CONFIG, "");
    properties.put(RESPONSIVE_LICENSE_CONFIG, "");
    properties.put(RESPONSIVE_LICENSE_FILE_CONFIG, "");
    properties.put(STORAGE_BACKEND_TYPE_CONFIG, StorageBackend.NONE.name());
    final StreamsBuilder builder = new StreamsBuilder();
    builder.stream("foo").to("bar");

    // when/then:
    assertThrows(
        ConfigException.class,
        () -> {
          final var ks = new ResponsiveKafkaStreams(builder.build(), properties, supplier);
          ks.close();
        }
    );
  }

  @Test
  public void shouldSkipLicenseCheckIfApiKeyConfigured() {
    // given:
    properties.put(PLATFORM_API_KEY_CONFIG, "some-api-key");
    properties.put(RESPONSIVE_LICENSE_CONFIG, "");
    properties.put(RESPONSIVE_LICENSE_FILE_CONFIG, "");
    properties.put(STORAGE_BACKEND_TYPE_CONFIG, StorageBackend.NONE.name());
    final StreamsBuilder builder = new StreamsBuilder();
    builder.stream("foo").to("bar");

    // when/then (no  throw):
    final var ks = new ResponsiveKafkaStreams(builder.build(), properties, supplier);
    ks.close();
  }

  private File writeLicenseFile(final String encoded) {
    try {
      final File encodedFile = File.createTempFile("rkst", null);
      encodedFile.deleteOnExit();
      Files.writeString(encodedFile.toPath(), encoded);
      return encodedFile;
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }
}