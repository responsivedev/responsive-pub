/*
 * Copyright 2025 Responsive Computing, Inc.
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.responsive.kafka.api.config.ResponsiveConfig;
import dev.responsive.kafka.internal.license.model.CloudLicenseV1;
import dev.responsive.kafka.internal.license.model.TimedTrialV1;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.Properties;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

class LicenseUtilsTest {

  @Test
  public void testFallBackToPlatformApiKey() {
    final var props = new Properties();
    props.put(ResponsiveConfig.PLATFORM_API_KEY_CONFIG, "test-api-key");

    final var config = ResponsiveConfig.responsiveConfig(props);
    final var license = LicenseUtils.loadLicense(config);
    assertThat(license, instanceOf(CloudLicenseV1.class));

    final var cloudLicense = (CloudLicenseV1) license;
    assertThat(cloudLicense.key(), is("test-api-key"));
  }

  @Test
  public void testLicenseShouldHavePrecedenceOverApiKey() throws Exception {
    final var props = new Properties();
    props.put(ResponsiveConfig.RESPONSIVE_LICENSE_CONFIG, testLicenseContent());
    props.put(ResponsiveConfig.PLATFORM_API_KEY_CONFIG, "test-api-key");

    final var config = ResponsiveConfig.responsiveConfig(props);
    final var license = LicenseUtils.loadLicense(config);
    assertThat(license, instanceOf(TimedTrialV1.class));
  }

  @Test
  public void testCannotSpecifyLicenseAndLicenseFile() throws Exception {
    final var props = new Properties();
    props.put(ResponsiveConfig.RESPONSIVE_LICENSE_CONFIG, testLicenseContent());
    props.put(ResponsiveConfig.RESPONSIVE_LICENSE_FILE_CONFIG, "/invalid/location");
    final var config = ResponsiveConfig.responsiveConfig(props);
    assertThrows(ConfigException.class, () -> LicenseUtils.loadLicense(config));
  }

  @Test
  public void testMustSpecifyLicenseOrApiKey() {
    final var props = new Properties();
    final var config = ResponsiveConfig.responsiveConfig(props);
    assertThrows(ConfigException.class, () -> LicenseUtils.loadLicense(config));
  }

  private String testLicenseContent() throws URISyntaxException, IOException {
    final URL resource = LicenseUtils.class.getResource("/test-licenses/test-trial-license.json");
    final var licenseBytes = Files.readAllBytes(Paths.get(resource.toURI()));
    return Base64.getEncoder().encodeToString(licenseBytes);
  }

}
