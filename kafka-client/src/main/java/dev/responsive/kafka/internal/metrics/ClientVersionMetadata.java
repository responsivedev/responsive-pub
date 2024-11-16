/*
 * Copyright 2024 Responsive Computing, Inc.
 *
 * This source code is licensed under the Responsive Software License Agreement v1.0 available at
 *
 * https://www.responsive.dev/legal/responsive-bsl-10
 *
 * This software requires a valid Commercial License Key for production use. Trial and commercial
 * licenses can be obtained at https://www.responsive.dev/sdk/get-started.
 */

package dev.responsive.kafka.internal.metrics;

import java.io.InputStream;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientVersionMetadata {
  private static final Logger LOG = LoggerFactory.getLogger(ClientVersionMetadata.class);

  // Do not remove the leading slash from these: it's needed (apparently) to
  // locate the property files from the resources of the respective builds
  public static final String RESPONSIVE_VERSION_FILE = "/version.properties";
  public static final String KAFKA_STREAMS_VERSION_FILE = "/kafka/kafka-streams-version.properties";

  public final String responsiveClientVersion;
  public final String responsiveClientCommitId;
  public final String streamsClientVersion;
  public final String streamsClientCommitId;

  public ClientVersionMetadata(
      final String responsiveClientVersion,
      final String responsiveClientCommitId,
      final String streamsClientVersion,
      final String streamsClientCommitId
  ) {
    this.responsiveClientVersion = responsiveClientVersion;
    this.responsiveClientCommitId = responsiveClientCommitId;
    this.streamsClientVersion = streamsClientVersion;
    this.streamsClientCommitId = streamsClientCommitId;
  }

  public static ClientVersionMetadata loadVersionMetadata() {
    final Properties responsiveProps = loadPropertiesFromFile(RESPONSIVE_VERSION_FILE);
    final String responsiveClientVersion =
        responsiveProps.getProperty("git.build.version", "").trim();
    final String responsiveClientCommitId =
        responsiveProps.getProperty("git.commit.id", "").trim();

    final Properties kafkaStreamsProps = loadPropertiesFromFile(KAFKA_STREAMS_VERSION_FILE);
    final String streamsClientVersion =
        kafkaStreamsProps.getProperty("version", "").trim();
    final String streamsClientCommitId =
        kafkaStreamsProps.getProperty("commitId", "").trim();

    return new ClientVersionMetadata(
        responsiveClientVersion,
        responsiveClientCommitId,
        streamsClientVersion,
        streamsClientCommitId
    );
  }

  private static Properties loadPropertiesFromFile(final String filePath) {
    final Properties props = new Properties();
    try (final InputStream resourceStream = ClientVersionMetadata.class
        .getResourceAsStream(filePath)
    ) {
      props.load(resourceStream);
      LOG.debug("Loaded version properties from file {}", filePath);
    } catch (final Exception exception) {
      LOG.warn("Error while loading file " + filePath, exception);
    }
    return props;
  }

  @Override
  public String toString() {
    return String.format(
        "Responsive Version: %s\n"
            + "Responsive Commit ID: %s\n"
            + "Kafka Streams Version: %s\n"
            + "Kafka Streams Commit ID: %s\n",
        responsiveClientVersion, responsiveClientCommitId,
        streamsClientVersion, streamsClientCommitId
    );
  }
}
