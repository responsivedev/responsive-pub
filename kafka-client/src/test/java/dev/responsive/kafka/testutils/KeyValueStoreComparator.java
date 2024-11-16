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

package dev.responsive.kafka.testutils;

import dev.responsive.kafka.internal.utils.Iterators;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

public class KeyValueStoreComparator<K, V> implements KeyValueStore<K, V> {
  private final KeyValueStore<K, V> sourceOfTruth;
  private final KeyValueStore<K, V> candidate;
  private final StoreComparatorSuppliers.CompareFunction compare;

  public KeyValueStoreComparator(KeyValueStore<K, V> sourceOfTruth, KeyValueStore<K, V> candidate) {
    this(
        sourceOfTruth,
        candidate,
        (String method, Object[] args, Object truth, Object actual) -> {
          System.out.printf("StoreComparator compare: %s | %s\n", method, args);
        }
    );
  }

  public KeyValueStoreComparator(
      KeyValueStore<K, V> sourceOfTruth, KeyValueStore<K, V> candidate,
      StoreComparatorSuppliers.CompareFunction compare
  ) {
    this.sourceOfTruth = sourceOfTruth;
    this.candidate = candidate;
    this.compare = compare;
  }

  @Override
  public void put(final K k, final V v) {
    this.sourceOfTruth.put(k, v);
    this.candidate.put(k, v);
  }

  @Override
  public V putIfAbsent(final K k, final V v) {
    final V sourceOfTruthResult = this.sourceOfTruth.putIfAbsent(k, v);
    final V candidateResult = this.candidate.putIfAbsent(k, v);
    this.compare.apply(
        "putIfAbsent", new Object[]{k, v},
        sourceOfTruthResult, candidateResult
    );
    return sourceOfTruthResult;
  }

  @Override
  public void putAll(final List<KeyValue<K, V>> list) {
    this.sourceOfTruth.putAll(list);
    this.candidate.putAll(list);
  }

  @Override
  public V delete(final K k) {
    final V sourceOfTruthResult = this.sourceOfTruth.delete(k);
    final V candidateResult = this.candidate.delete(k);
    this.compare.apply(
        "delete", new Object[]{k},
        sourceOfTruthResult, candidateResult
    );
    return sourceOfTruthResult;
  }

  @Override
  public V get(final K k) {
    final V sourceOfTruthResult = this.sourceOfTruth.get(k);
    final V candidateResult = this.candidate.get(k);
    this.compare.apply(
        "get", new Object[]{k},
        sourceOfTruthResult, candidateResult
    );
    return sourceOfTruthResult;
  }

  @Override
  public KeyValueIterator<K, V> range(final K k, final K k1) {
    final var sourceOfTruthIterator = this.sourceOfTruth.range(k, k1);
    final var candidateIterator = this.candidate.range(k, k1);

    List<KeyValue<K, V>> expectedResults = new ArrayList<>();
    List<KeyValue<K, V>> actualResults = new ArrayList<>();
    while (sourceOfTruthIterator.hasNext()) {
      expectedResults.add(sourceOfTruthIterator.next());
    }
    while (candidateIterator.hasNext()) {
      actualResults.add(candidateIterator.next());
    }

    this.compare.apply(
        "range", new Object[] {k, k1},
        actualResults.size(), expectedResults.size()
    );
    for (var i = 0; i < actualResults.size(); i++) {
      var actual = actualResults.get(i);
      var expected = expectedResults.get(i);

      this.compare.apply(
          "range", new Object[] {k, k1},
          actual.key, expected.key
      );
      this.compare.apply(
          "range", new Object[] {k, k1},
          actual.value, expected.value
      );
    }

    return Iterators.kv(actualResults.iterator(), el -> el);
  }

  @Override
  public KeyValueIterator<K, V> all() {
    final var sourceOfTruthIterator = this.sourceOfTruth.all();
    final var candidateIterator = this.candidate.all();

    List<KeyValue<K, V>> expectedResults = new ArrayList<>();
    List<KeyValue<K, V>> actualResults = new ArrayList<>();
    while (sourceOfTruthIterator.hasNext()) {
      expectedResults.add(sourceOfTruthIterator.next());
    }
    while (candidateIterator.hasNext()) {
      actualResults.add(candidateIterator.next());
    }

    this.compare.apply(
        "all", new Object[] {},
        actualResults.size(), expectedResults.size()
    );
    for (var i = 0; i < actualResults.size(); i++) {
      var actual = actualResults.get(i);
      var expected = expectedResults.get(i);

      this.compare.apply(
          "all", new Object[] {},
          actual.key, expected.key
      );
      this.compare.apply(
          "all", new Object[] {},
          actual.value, expected.value
      );
    }

    return Iterators.kv(actualResults.iterator(), el -> el);
  }

  @Override
  public String name() {
    return this.sourceOfTruth.name();
  }

  @Override
  @Deprecated
  public void init(final ProcessorContext context, final StateStore root) {
    if (context instanceof StateStoreContext) {
      init((StateStoreContext) context, root);
    } else {
      throw new UnsupportedOperationException(
          "Use ResponsiveSessionStore#init(StateStoreContext, StateStore) instead."
      );
    }
  }

  @Override
  public void init(final StateStoreContext context, final StateStore root) {
    StateStoreContext proxy = (StateStoreContext) Proxy.newProxyInstance(
        InternalProcessorContext.class.getClassLoader(),
        new Class<?>[] {InternalProcessorContext.class},
        new MultiStateStoreContext(context, root)
    );
    this.sourceOfTruth.init(proxy, root);
    this.candidate.init(proxy, root);
  }

  @Override
  public void flush() {
    this.sourceOfTruth.flush();
    this.candidate.flush();
  }

  @Override
  public void close() {
    this.sourceOfTruth.close();
    this.candidate.close();
  }

  @Override
  public long approximateNumEntries() {
    return this.sourceOfTruth.approximateNumEntries();
  }

  @Override
  public boolean persistent() {
    return this.sourceOfTruth.persistent() || this.candidate.persistent();
  }

  @Override
  public boolean isOpen() {
    this.compare.apply(
        "isOpen", new Object[] {},
        this.candidate.isOpen(), this.sourceOfTruth.isOpen()
    );
    return this.sourceOfTruth.isOpen();
  }

  @Override
  public Position getPosition() {
    return this.sourceOfTruth.getPosition();
  }
}
