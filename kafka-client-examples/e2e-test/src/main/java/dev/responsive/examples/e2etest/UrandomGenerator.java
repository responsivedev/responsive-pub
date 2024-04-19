package dev.responsive.examples.e2etest;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.Base64;

public class UrandomGenerator {
  private static final File URANDOM = new File("/dev/urandom");
  private static final int READ_SIZE = 32 * 1024;

  private ByteBuffer buffer;

  public UrandomGenerator() {
    reloadBuffer();
  }

  public synchronized Long nextLong() {
    try {
      return buffer.getLong();
    } catch (final BufferUnderflowException e) {
      reloadBuffer();
      return buffer.getLong();
    }
  }

  public String nextString(final int sz) {
    final int readSz = sz / 4 * 3;
    if (readSz > READ_SIZE) {
      throw new IllegalArgumentException();
    }
    final byte[] data = new byte[readSz];
    try {
      buffer.get(data);
    } catch (final BufferUnderflowException e) {
      reloadBuffer();
      buffer.get(data);
    }
    return Base64.getEncoder().encodeToString(data);
  }

  private void reloadBuffer() {
    try (final var stream = new FileInputStream(URANDOM)) {
      final var rawRandom = stream.readNBytes(READ_SIZE);
      buffer = ByteBuffer.wrap(rawRandom);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static void main(final String[] args) {
    final var generator = new UrandomGenerator();
    for (int i = 0; i < 10; i++) {
      System.out.println(generator.nextLong());
    }
  }
}
