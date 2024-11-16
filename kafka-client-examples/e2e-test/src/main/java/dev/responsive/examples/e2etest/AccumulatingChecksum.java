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

package dev.responsive.examples.e2etest;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.function.Supplier;

public class AccumulatingChecksum {
  private final Supplier<MessageDigest> digestSupplier = () -> {
    try {
      return MessageDigest.getInstance("SHA-256");
    } catch (final NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  };
  private final byte[] current;

  public AccumulatingChecksum() {
    current = new byte[0];
  }

  public AccumulatingChecksum(final byte[] previous) {
    this.current = previous;
  }

  public AccumulatingChecksum updateWith(final long value) {
    final var digest = digestSupplier.get();
    digest.update(current);
    final byte[] buf = new byte[Long.BYTES];
    final ByteBuffer wrapped = ByteBuffer.wrap(buf);
    wrapped.putLong(value);
    digest.update(buf);
    return new AccumulatingChecksum(digest.digest());
  }

  public byte[] current() {
    return current;
  }
}
