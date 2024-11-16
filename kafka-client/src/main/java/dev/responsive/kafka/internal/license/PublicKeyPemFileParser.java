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

package dev.responsive.kafka.internal.license;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Base64;
import java.util.List;

public class PublicKeyPemFileParser {
  private static final String HEADER_PREFIX = "-----";
  private static final String BEGIN_PUBLIC_KEY = "BEGIN PUBLIC KEY";
  private static final String END_PUBLIC_KEY = "END PUBLIC KEY";
  private static final String BEGIN_PUBLIC_KEY_HEADER
      = HEADER_PREFIX + BEGIN_PUBLIC_KEY + HEADER_PREFIX;
  private static final String END_PUBLIC_KEY_HEADER
      = HEADER_PREFIX + END_PUBLIC_KEY + HEADER_PREFIX;

  public static byte[] parsePemFile(final File file) {
    final List<String> lines;
    try {
      lines = Files.readAllLines(file.toPath());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    final StringBuilder keyB64Builder = new StringBuilder();
    boolean foundBegin = false;
    for (final String l : lines) {
      if (l.equals(BEGIN_PUBLIC_KEY_HEADER)) {
        foundBegin = true;
      } else if (foundBegin) {
        if (l.equals(END_PUBLIC_KEY_HEADER)) {
          final String keyB64 = keyB64Builder.toString();
          return Base64.getDecoder().decode(keyB64);
        }
        keyB64Builder.append(l);
      }
    }
    throw new IllegalArgumentException("invalid public key pem");
  }
}
