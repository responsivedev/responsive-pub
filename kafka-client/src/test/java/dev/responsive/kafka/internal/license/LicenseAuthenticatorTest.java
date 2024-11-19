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

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.responsive.kafka.internal.license.exception.LicenseAuthenticationException;
import dev.responsive.kafka.internal.license.model.LicenseDocument;
import dev.responsive.kafka.internal.license.model.SigningKeys;
import java.io.IOException;
import org.junit.jupiter.api.Test;

class LicenseAuthenticatorTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final LicenseAuthenticator verifier = new LicenseAuthenticator(loadSigningKeys());

  @Test
  public void shouldVerifyLicense() {
    // given:
    final LicenseDocument license = loadLicense("test-license.json");

    // when/then (no throw):
    verifier.authenticate(license);
  }

  @Test
  public void shouldThrowForFailedSignatureVerification() {
    // given:
    final LicenseDocument license = loadLicense("test-license-invalid-signature.json");

    // when/then:
    assertThrows(
        LicenseAuthenticationException.class,
        () -> verifier.authenticate(license)
    );
  }

  private static LicenseDocument loadLicense(final String file) {
    return loadResource("/test-licenses/" + file, LicenseDocument.class);
  }

  private static SigningKeys loadSigningKeys() {
    return loadResource("/responsive-license-keys/license-keys.json", SigningKeys.class);
  }

  private static <T> T loadResource(final String path, final Class<T> clazz) {
    try {
      return MAPPER.readValue(
          LicenseAuthenticatorTest.class.getResource(path),
          clazz
      );
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }
}