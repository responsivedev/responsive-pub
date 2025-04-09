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

import java.util.Objects;

public interface RangeBound<K> {

  <T> T map(Mapper<K, T> mapper);

  static <K> Unbounded<K> unbounded() {
    return new Unbounded<>();
  }

  static <K> InclusiveBound<K> inclusive(K key) {
    return new InclusiveBound<>(key);
  }

  static <K> ExclusiveBound<K> exclusive(K key) {
    return new ExclusiveBound<>(key);
  }

  class InclusiveBound<K> implements RangeBound<K> {
    private final K key;

    public InclusiveBound(final K key) {
      this.key = key;
    }

    public K key() {
      return key;
    }

    @Override
    public <T> T map(final Mapper<K, T> mapper) {
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
      final InclusiveBound<?> that = (InclusiveBound<?>) o;
      return Objects.equals(key, that.key);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(key);
    }
  }

  class ExclusiveBound<K> implements RangeBound<K> {
    private final K key;

    public ExclusiveBound(final K key) {
      this.key = key;
    }

    public K key() {
      return key;
    }

    @Override
    public <T> T map(final Mapper<K, T> mapper) {
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
      final ExclusiveBound<?> that = (ExclusiveBound<?>) o;
      return Objects.equals(key, that.key);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(key);
    }
  }

  class Unbounded<K> implements RangeBound<K> {
    private Unbounded() {}

    @Override
    public <T> T map(final Mapper<K, T> mapper) {
      return mapper.map(this);
    }
  }

  interface Mapper<K, T> {
    T map(InclusiveBound<K> b);

    T map(ExclusiveBound<K> b);

    T map(Unbounded<K> b);
  }

}
