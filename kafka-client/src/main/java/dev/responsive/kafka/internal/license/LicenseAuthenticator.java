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

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.responsive.kafka.internal.license.exception.LicenseAuthenticationException;
import dev.responsive.kafka.internal.license.model.LicenseDocument;
import dev.responsive.kafka.internal.license.model.LicenseDocumentV1;
import dev.responsive.kafka.internal.license.model.LicenseInfo;
import dev.responsive.kafka.internal.license.model.SigningKeys;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.Signature;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.MGF1ParameterSpec;
import java.security.spec.PSSParameterSpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Objects;

public class LicenseAuthenticator {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final SigningKeys signingKeys;

  public LicenseAuthenticator(final SigningKeys signingKeys) {
    this.signingKeys = Objects.requireNonNull(signingKeys);
  }

  public LicenseInfo authenticate(final LicenseDocument license) {
    if (license instanceof LicenseDocumentV1) {
      return authenticateLicenseV1((LicenseDocumentV1) license);
    } else {
      throw new IllegalArgumentException(
          "unrecognized license doc type: " + license.getClass().getName());
    }
  }

  private LicenseInfo authenticateLicenseV1(final LicenseDocumentV1 license) {
    final byte[] infoBytes = verifyLicenseV1Signature(license);
    try {
      return MAPPER.readValue(infoBytes, LicenseInfo.class);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  private byte[] verifyLicenseV1Signature(final LicenseDocumentV1 license) {
    if (!license.algo().equals("RSASSA_PSS_SHA_256")) {
      throw new IllegalArgumentException("unrecognized license algo: " + license.algo());
    }
    final PublicKey publicKey = loadPublicKey(signingKeys.lookupKey(license.key()));
    final Signature signature;
    try {
      signature = Signature.getInstance("RSASSA-PSS");
      signature.setParameter(
          new PSSParameterSpec("SHA-256", "MGF1", MGF1ParameterSpec.SHA256, 32, 1)
      );
      signature.initVerify(publicKey);
      final byte[] info = license.decodeInfo();
      signature.update(info);
      if (!signature.verify(license.decodeSignature())) {
        throw new LicenseAuthenticationException("license info did not match signature");
      }
      return info;
    } catch (GeneralSecurityException e) {
      throw new RuntimeException(e);
    }
  }

  private PublicKey loadPublicKey(final SigningKeys.SigningKey signingKey) {
    final byte[] publicKeyBytes = PublicKeyPemFileParser.parsePemFileInResource(signingKey.path());
    final KeyFactory keyFactory;
    try {
      keyFactory = KeyFactory.getInstance("RSA");
    } catch (final NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
    final X509EncodedKeySpec keySpec = new X509EncodedKeySpec(publicKeyBytes);
    try {
      return keyFactory.generatePublic(keySpec);
    } catch (final InvalidKeySpecException e) {
      throw new RuntimeException(e);
    }
  }
}
