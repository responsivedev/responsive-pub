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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class OrderPreservingBase64EncoderTest {
  private final OrderPreservingBase64Encoder encoder = new OrderPreservingBase64Encoder();

  @Test
  public void shouldEncodeWithZeroBitsAsPadding() {
    // -1 is 0b11111111.
    // This means the base64 indexes are 0b111111 = 63 => 'z' and 0b110000 = 48 => 'k'
    assertThat(
        encoder.encode(new byte[]{1, 2, 3, 4, 5, 6, -1}), equalTo("+E61/+I4zk"));
  }

  @Test
  public void shouldEncodeUniquelyAndInOrder() {
    final ByteArrayGenerator generator = new ByteArrayGenerator(4, (byte) 1);
    String previous = null;
    for (int i = 0; i < (Math.pow(2, 21)); i++) {
      final Optional<byte[]> next = generator.next();
      if (next.isEmpty()) {
        break;
      }
      final String encoded = encoder.encode(next.get());
      if (previous != null) {
        assertThat(encoded, greaterThan(previous));
      }
      assertThat(encoder.decode(encoded), equalTo(next.get()));
      previous = encoded;
    }
  }

  private static class ByteArrayGenerator {
    final int bytes;
    final byte increment;
    final List<Byte> stack;
    byte[] next;
    String previous = "";
    int count = 0;

    public ByteArrayGenerator(final int bytes, final byte increment) {
      this.bytes = bytes;
      this.increment = increment;
      stack = new ArrayList<>(bytes);
      assertThat(bytes, lessThan(8));
      for (int i = 0; i < bytes; i++) {
        stack.add(Byte.MIN_VALUE);
      }
      next = convertFromStack();
    }

    public Optional<byte[]> next() {
      count++;
      if (next == null) {
        return Optional.empty();
      }
      final byte[] ret = next;
      for (int i = bytes - 1; i >= 0; i--) {
        if ((Byte.MAX_VALUE - increment) < stack.get(i)) {
          stack.set(i, Byte.MIN_VALUE);
          if (i == 0) {
            next = null;
          }
        } else {
          stack.set(i, (byte) (stack.get(i) + increment));
          next = convertFromStack();
          final String bitString = toBitString(next);
          assertThat(bitString, greaterThan(previous));
          previous = bitString;
          break;
        }
      }
      return Optional.of(ret);
    }

    private byte[] convertFromStack() {
      final byte[] asArray = new byte[bytes];
      for (int i = 0; i < bytes; i++) {
        asArray[i] = shift(stack.get(i));
      }
      return asArray;
    }

    private byte shift(final byte in) {
      int shifted = ((int) in + Byte.MAX_VALUE + 1);
      if (shifted > Byte.MAX_VALUE) {
        shifted = Byte.MIN_VALUE + (shifted - Byte.MAX_VALUE) - 1;
      }
      return (byte) shifted;
    }

    private String toBitString(final byte[] bytes) {
      final byte[] lng = new byte[8];
      final ByteBuffer bb = ByteBuffer.wrap(lng);
      bb.put(Byte.MIN_VALUE);
      for (int i = 1; i < 8 - bytes.length; i++) {
        bb.put((byte) 0);
      }
      bb.put(bytes);
      bb.rewind();
      final long asLong = bb.getLong();
      return Long.toBinaryString(asLong);
    }
  }
}