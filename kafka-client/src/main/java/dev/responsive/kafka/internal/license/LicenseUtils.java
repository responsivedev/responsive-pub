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

package dev.responsive.kafka.internal.license;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.responsive.kafka.api.ResponsiveKafkaStreams;
import dev.responsive.kafka.api.config.ResponsiveConfig;
import dev.responsive.kafka.internal.license.model.CloudLicenseV1;
import dev.responsive.kafka.internal.license.model.LicenseDocument;
import dev.responsive.kafka.internal.license.model.LicenseInfo;
import dev.responsive.kafka.internal.license.model.SigningKeys;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Base64;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;

public final class LicenseUtils {

  private static final String SIGNING_KEYS_PATH = "/responsive-license-keys/license-keys.json";

  private LicenseUtils() {
  }

  public static LicenseInfo loadLicense(final ResponsiveConfig config) {
    if (!config.getString(ResponsiveConfig.PLATFORM_API_KEY_CONFIG).isEmpty()) {
      // for now, we don't do any additional validation for users that use
      // Responsive Cloud via the platform key pair since that will be validated
      // when they make any request to the controller
      return new CloudLicenseV1(
          CloudLicenseV1.TYPE_NAME,
          config.getString(ResponsiveConfig.PLATFORM_API_KEY_CONFIG)
      );
    }

    final var licenseDoc = loadLicenseDocument(config);
    final var licenseInfo = authenticateLicense(licenseDoc);
    final LicenseChecker checker = new LicenseChecker();
    checker.checkLicense(licenseInfo);
    return licenseInfo;
  }

  private static LicenseDocument loadLicenseDocument(final ResponsiveConfig configs) {
    final Password licensePass = configs.getPassword(ResponsiveConfig.RESPONSIVE_LICENSE_CONFIG);
    final String license = licensePass == null ? "" : licensePass.value();
    final String licenseFile = configs.getString(ResponsiveConfig.RESPONSIVE_LICENSE_FILE_CONFIG);
    if (license.isEmpty() == licenseFile.isEmpty()) {
      throw new ConfigException(String.format(
          "Must set exactly one of %s or %s",
          ResponsiveConfig.RESPONSIVE_LICENSE_CONFIG,
          ResponsiveConfig.RESPONSIVE_LICENSE_FILE_CONFIG
      ));
    }
    final String licenseB64;
    if (!license.isEmpty()) {
      licenseB64 = license;
    } else {
      try {
        licenseB64 = Files.readString(new File(licenseFile).toPath());
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
    }
    final ObjectMapper mapper = new ObjectMapper();
    try {
      return mapper.readValue(Base64.getDecoder().decode(licenseB64), LicenseDocument.class);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static LicenseInfo authenticateLicense(final LicenseDocument document) {
    final SigningKeys signingKeys = loadSigningKeys();
    final LicenseAuthenticator licenseAuthenticator = new LicenseAuthenticator(signingKeys);
    return licenseAuthenticator.authenticate(document);
  }

  private static SigningKeys loadSigningKeys() {
    try {
      return new ObjectMapper().readValue(
          ResponsiveKafkaStreams.class.getResource(SIGNING_KEYS_PATH),
          SigningKeys.class
      );
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

}
