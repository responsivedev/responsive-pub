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

package dev.responsive.kafka.internal.db.rs3.client;

import java.util.Arrays;
import java.util.Objects;

public interface RangeBound {

  <T> T map(Mapper<T> mapper);

  static Unbounded unbounded() {
    return Unbounded.INSTANCE;
  }

  static InclusiveBound inclusive(byte[] key) {
    return new InclusiveBound(key);
  }

  static ExclusiveBound exclusive(byte[] key) {
    return new ExclusiveBound(key);
  }

  class InclusiveBound implements RangeBound {
    private final byte[] key;

    public InclusiveBound(final byte[] key) {
      this.key = key;
    }

    public byte[] key() {
      return key;
    }

    @Override
    public <T> T map(final Mapper<T> mapper) {
      return mapper.map(this);
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final InclusiveBound that = (InclusiveBound) o;
      return Objects.deepEquals(key, that.key);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(key);
    }
  }

  class ExclusiveBound implements RangeBound {
    private final byte[] key;

    public ExclusiveBound(final byte[] key) {
      this.key = key;
    }

    public byte[] key() {
      return key;
    }

    @Override
    public <T> T map(final Mapper<T> mapper) {
      return mapper.map(this);
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final ExclusiveBound that = (ExclusiveBound) o;
      return Objects.deepEquals(key, that.key);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(key);
    }
  }

  class Unbounded implements RangeBound {
    private static final Unbounded INSTANCE = new Unbounded();

    private Unbounded() {}

    @Override
    public <T> T map(final Mapper<T> mapper) {
      return mapper.map(this);
    }
  }

  interface Mapper<T> {
    T map(InclusiveBound b);

    T map(ExclusiveBound b);

    T map(Unbounded b);
  }

}
