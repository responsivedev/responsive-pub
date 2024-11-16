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

package dev.responsive.kafka.internal.utils;

import static java.util.Collections.emptyIterator;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.WindowStoreIterator;

/**
 * Utility class for creating iterators.
 */
public final class Iterators {

  private Iterators() {}

  public static <K, V> KeyValueIterator<K, V> emptyKv() {
    return Iterators.kv(emptyIterator(), e -> new KeyValue<>(null, null));
  }

  /**
   * Returns an iterator that caches the last value returned by
   * a delegate so that {@code peek()} can check the next key
   * without exhausting the iterator.
   *
   * @param delegate the delegate
   * @return a new iterator of the same type as delegate that
   *         supports {@link PeekingIterator#peek()}
   */
  public static <T> PeekingIterator<T> peeking(final Iterator<T> delegate) {
    if (delegate instanceof PeekingIterator) {
      return (PeekingIterator<T>) delegate;
    }

    return new PeekingIterator<>(delegate);
  }

  public static <T> Iterator<T> filter(
      final Iterator<T> delegate,
      final Predicate<T> filter
  ) {
    return new FilterIterator<>(delegate, filter);
  }

  public static <K, V> KeyValueIterator<K, V> filterKv(
      final KeyValueIterator<K, V> delegate,
      final Predicate<K> filter
  ) {
    return new KvFilterIterator<>(delegate, filter);
  }

  /**
   * Transforms the delegate iterator into a {@link KeyValueIterator}
   * by applying the transformation method {@code extract(T)} passed
   * in.
   */
  public static <T, K, V> KeyValueIterator<K, V> kv(
      final Iterator<T> delegate,
      final Function<T, KeyValue<K, V>> extract
  ) {
    return new TransformIterator<>(delegate, extract);
  }

  /**
   * Wraps a {@link KeyValueIterator} that already returns what the
   * {@link WindowStoreIterator} requires but using {@link WindowedKey}
   * instead of {@code Long}.
   */
  public static WindowStoreIterator<byte[]> windowed(
      final KeyValueIterator<WindowedKey, byte[]> delegate
  ) {
    return new WindowIterator(delegate);
  }

  /**
   * Returns an iterator that contains window ends instead of just
   * the stamped start timestamp.
   */
  public static KeyValueIterator<Windowed<Bytes>, byte[]> windowedKey(
      final KeyValueIterator<WindowedKey, byte[]> delegate,
      final long windowSize
  ) {
    return new WindowKeyIterator(delegate, windowSize);
  }

  /**
   * Returns an iterator that iterates over all delegates in order
   */
  public static <K extends Comparable<K>, V> KeyValueIterator<K, V> wrapped(
      final List<KeyValueIterator<K, V>> delegates
  ) {
    return new MultiPartitionRangeIterator<>(delegates);
  }

  public static <I, O, V> KeyValueIterator<O, V> mapKeys(
      final KeyValueIterator<I, V> delegate,
      final Function<I, O> mapper
  ) {
    return new KeyMappingIterator<>(delegate, mapper);
  }

  private static class PeekingIterator<T> implements Iterator<T> {

    private final Iterator<T> delegate;
    private T cached;

    private PeekingIterator(final Iterator<T> delegate) {
      this.delegate = delegate;
    }

    public T peek() {
      cache();
      return cached;
    }

    @Override
    public boolean hasNext() {
      cache();
      return cached != null;
    }

    @Override
    public T next() {
      cache();
      final T next = cached;
      cached = null;
      return next;
    }

    private void cache() {
      if (cached == null && delegate.hasNext()) {
        cached = delegate.next();
      }
    }
  }

  private static class TransformIterator<T, K, V> implements KeyValueIterator<K, V> {

    private final PeekingIterator<T> delegate;
    private final Function<T, KeyValue<K, V>> extract;

    private TransformIterator(
        final Iterator<T> delegate,
        final Function<T, KeyValue<K, V>> extract
    ) {
      if (delegate instanceof KeyValueIterator) {
        throw new IllegalArgumentException("TransformIterator should not wrap KeyValueIterators");
      }

      this.delegate = peeking(delegate);
      this.extract = extract;
    }

    @Override
    public void close() {
    }

    @Override
    public K peekNextKey() {
      return extract.apply(delegate.peek()).key;
    }

    @Override
    public boolean hasNext() {
      return delegate.hasNext();
    }

    @Override
    public KeyValue<K, V> next() {
      return extract.apply(delegate.next());
    }
  }

  private static class WindowIterator implements WindowStoreIterator<byte[]> {

    private final KeyValueIterator<WindowedKey, byte[]> delegate;

    public WindowIterator(final KeyValueIterator<WindowedKey, byte[]> delegate) {
      this.delegate = delegate;
    }

    @Override
    public void close() {
      delegate.close();
    }

    @Override
    public Long peekNextKey() {
      return delegate.peekNextKey().windowStartMs;
    }

    @Override
    public boolean hasNext() {
      return delegate.hasNext();
    }

    @Override
    public KeyValue<Long, byte[]> next() {
      final KeyValue<WindowedKey, byte[]> next = delegate.next();
      return new KeyValue<>(next.key.windowStartMs, next.value);
    }
  }

  private static class KvFilterIterator<K, V> implements KeyValueIterator<K, V> {

    private final KeyValueIterator<K, V> delegate;
    private final Predicate<K> filter;

    private KvFilterIterator(
        final KeyValueIterator<K, V> delegate,
        final Predicate<K> filter
    ) {
      this.delegate = delegate;
      this.filter = filter;
    }

    @Override
    public void close() {
      delegate.close();
    }

    @Override
    public K peekNextKey() {
      if (hasNext()) {
        return delegate.peekNextKey();
      }

      return null;
    }

    @Override
    public boolean hasNext() {
      while (delegate.hasNext()) {
        if (filter.test(delegate.peekNextKey())) {
          return true;
        }

        // otherwise filter it out and try again
        delegate.next();
      }

      return false;
    }

    @Override
    public KeyValue<K, V> next() {
      if (hasNext()) {
        return delegate.next();
      }
      throw new NoSuchElementException();
    }
  }

  private static class FilterIterator<T> implements Iterator<T> {

    private final PeekingIterator<T> delegate;
    private final Predicate<T> filter;

    private FilterIterator(final Iterator<T> delegate, final Predicate<T> filter) {
      this.delegate = peeking(delegate);
      this.filter = filter;
    }

    @Override
    public boolean hasNext() {
      while (delegate.hasNext()) {
        if (filter.test(delegate.peek())) {
          return true;
        }

        // otherwise filter it out and try again
        delegate.next();
      }

      return false;
    }

    @Override
    public T next() {
      if (hasNext()) {
        return delegate.next();
      }
      throw new NoSuchElementException();
    }
  }

  private static class KeyMappingIterator<I, O, V> implements KeyValueIterator<O, V> {

    private final KeyValueIterator<I, V> delegate;
    private final Function<I, O> mapper;

    private KeyMappingIterator(final KeyValueIterator<I, V> delegate, final Function<I, O> mapper) {
      this.delegate = delegate;
      this.mapper = mapper;
    }

    @Override
    public boolean hasNext() {
      return delegate.hasNext();
    }

    @Override
    public KeyValue<O, V> next() {
      if (hasNext()) {
        final var next = delegate.next();
        return new KeyValue<>(mapper.apply(next.key), next.value);
      }
      throw new NoSuchElementException();
    }

    @Override
    public void close() {
      delegate.close();
    }

    @Override
    public O peekNextKey() {
      final var next = delegate.peekNextKey();
      return mapper.apply(next);
    }
  }

  private static class WindowKeyIterator implements KeyValueIterator<Windowed<Bytes>, byte[]> {

    private final KeyValueIterator<WindowedKey, byte[]> delegate;
    private final long windowSize;

    private WindowKeyIterator(
        final KeyValueIterator<WindowedKey, byte[]> delegate,
        final long windowSize
    ) {
      this.delegate = delegate;
      this.windowSize = windowSize;
    }

    @Override
    public void close() {
      delegate.close();
    }

    @Override
    public Windowed<Bytes> peekNextKey() {
      return fromStamp(delegate.peekNextKey());
    }

    @Override
    public boolean hasNext() {
      return delegate.hasNext();
    }

    @Override
    public KeyValue<Windowed<Bytes>, byte[]> next() {
      final KeyValue<WindowedKey, byte[]> next = delegate.next();
      return new KeyValue<>(
          fromStamp(next.key),
          next.value
      );
    }

    private Windowed<Bytes> fromStamp(final WindowedKey windowedKey) {
      return new Windowed<>(
          windowedKey.key,
          new TimeWindow(windowedKey.windowStartMs, endTs(windowedKey.windowStartMs))
      );
    }

    private long endTs(final long stamp) {
      final long endMs = stamp + windowSize;
      return endMs < 0 ? Long.MAX_VALUE : endMs;
    }
  }
}