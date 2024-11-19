/*
 * Copyright 2024 Responsive Computing, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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