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

package dev.responsive.kafka.internal.db.mongo;

import java.util.HashSet;
import java.util.Set;
import org.apache.commons.codec.binary.Base64;

public class OrderPreservingBase64Encoder {
  private static final Base64 encoder = Base64.builder()
      .setEncodeTable(
          encoderCharset("+/0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"))
      .get();

  public String encode(final byte[] input) {
    return encoder.encodeToString(input);
  }

  public byte[] decode(final String input) {
    return encoder.decode(input);
  }

  private static byte[] encoderCharset(final String charset) {
    if (charset.length() != 64) {
      throw new IllegalStateException("CHARSET has unexpected length: " + charset.length());
    }
    final char[] asCharArray = charset.toCharArray();
    final Set<Character> asSet = new HashSet<>(64);
    for (final var c : asCharArray) {
      asSet.add(c);
    }
    if (asSet.size() != 64) {
      throw new IllegalStateException("CHARSET contains duplicate characters");
    }
    final byte[] table = new byte[64];
    table[0] = (byte) asCharArray[0];
    for (int i = 1; i < 64; i++) {
      if (asCharArray[i] < asCharArray[i - 1]) {
        throw new IllegalStateException(String.format(
            "CHARSET contains out of order characters %s %s", asCharArray[i - 1], asCharArray[i]));
      }
      table[i] = (byte) asCharArray[i];
    }
    return table;
  }
}
