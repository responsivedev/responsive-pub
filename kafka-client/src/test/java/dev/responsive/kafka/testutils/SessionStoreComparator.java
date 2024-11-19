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
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.SessionStore;

public class SessionStoreComparator<K, V> implements SessionStore<K, V> {
  private final SessionStore<K, V> sourceOfTruth;
  private final SessionStore<K, V> candidate;
  private final StoreComparatorSuppliers.CompareFunction compare;

  public SessionStoreComparator(SessionStore<K, V> sourceOfTruth, SessionStore<K, V> candidate) {
    this(
        sourceOfTruth,
        candidate,
        (String method, Object[] args, Object truth, Object actual) -> {
          System.out.printf("StoreComparator compare: %s | %s", method, args);
        }
    );
  }

  public SessionStoreComparator(
      SessionStore<K, V> sourceOfTruth, SessionStore<K, V> candidate,
      StoreComparatorSuppliers.CompareFunction compare
  ) {
    this.sourceOfTruth = sourceOfTruth;
    this.candidate = candidate;
    this.compare = compare;
  }

  @Override
  public void remove(final Windowed<K> windowed) {
    this.sourceOfTruth.remove(windowed);
    this.candidate.remove(windowed);
  }

  @Override
  public void put(final Windowed<K> windowed, final V bytes) {
    this.sourceOfTruth.put(windowed, bytes);
    this.candidate.put(windowed, bytes);
  }

  @Override
  public String name() {
    return this.sourceOfTruth.name();
  }

  @Override
  public void init(final StateStoreContext storeContext, final StateStore root) {
    StateStoreContext proxy = (StateStoreContext) Proxy.newProxyInstance(
        InternalProcessorContext.class.getClassLoader(),
        new Class<?>[] {InternalProcessorContext.class},
        new MultiStateStoreContext(storeContext, root)
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
  public boolean persistent() {
    return this.sourceOfTruth.persistent() || this.candidate.persistent();
  }

  @Override
  public boolean isOpen() {
    this.compare.apply(
        "isOpen", new Object[] {},
        this.candidate.isOpen(), this.sourceOfTruth.isOpen()
    );
    return this.candidate.isOpen();
  }

  @Override
  public Position getPosition() {
    this.compare.apply(
        "getPosition", new Object[] {},
        this.candidate.getPosition(), this.sourceOfTruth.getPosition()
    );
    return this.candidate.getPosition();
  }

  @Override
  public KeyValueIterator<Windowed<K>, V> fetch(final K bytes) {
    final var sourceOfTruthIterator = this.sourceOfTruth.fetch(bytes);
    final var candidateIterator = this.candidate.fetch(bytes);

    List<KeyValue<Windowed<K>, V>> expectedResults = new ArrayList<>();
    List<KeyValue<Windowed<K>, V>> actualResults = new ArrayList<>();
    while (sourceOfTruthIterator.hasNext()) {
      expectedResults.add(sourceOfTruthIterator.next());
    }
    while (candidateIterator.hasNext()) {
      actualResults.add(candidateIterator.next());
    }

    this.compare.apply(
        "fetch", new Object[] {bytes},
        actualResults.size(), expectedResults.size()
    );
    for (var i = 0; i < actualResults.size(); i++) {
      var actual = actualResults.get(i);
      var expected = expectedResults.get(i);

      this.compare.apply(
          "fetch", new Object[] {bytes},
          actual.key.key(), expected.key.key()
      );
      this.compare.apply(
          "fetch", new Object[] {bytes},
          actual.value, expected.value
      );
    }

    return Iterators.kv(actualResults.iterator(), el -> el);
  }

  @Override
  public KeyValueIterator<Windowed<K>, V> fetch(final K bytes, final K k1) {
    final var sourceOfTruthIterator = this.sourceOfTruth.fetch(bytes, k1);
    final var candidateIterator = this.candidate.fetch(bytes, k1);

    List<KeyValue<Windowed<K>, V>> expectedResults = new ArrayList<>();
    List<KeyValue<Windowed<K>, V>> actualResults = new ArrayList<>();
    while (sourceOfTruthIterator.hasNext()) {
      expectedResults.add(sourceOfTruthIterator.next());
    }
    while (candidateIterator.hasNext()) {
      actualResults.add(candidateIterator.next());
    }

    this.compare.apply(
        "fetch", new Object[] {bytes, k1},
        actualResults.size(), expectedResults.size()
    );
    for (var i = 0; i < actualResults.size(); i++) {
      var actual = actualResults.get(i);
      var expected = expectedResults.get(i);

      this.compare.apply(
          "fetch", new Object[] {bytes, k1},
          actual.key.key(), expected.key.key()
      );
      this.compare.apply(
          "fetch", new Object[] {bytes, k1},
          actual.value, expected.value
      );
    }

    return Iterators.kv(actualResults.iterator(), el -> el);
  }

  @Override
  public V fetchSession(
      final K key,
      final long sessionStartTime,
      final long sessionEndTime
  ) {
    final var sourceOfTruthResult =
        this.sourceOfTruth.fetchSession(key, sessionStartTime, sessionEndTime);
    final var candidateResult =
        this.candidate.fetchSession(key, sessionStartTime, sessionEndTime);
    this.compare.apply(
        "fetchSession", new Object[] {key, sessionStartTime, sessionEndTime},
        candidateResult, sourceOfTruthResult
    );
    return candidateResult;
  }

  @Override
  public KeyValueIterator<Windowed<K>, V> findSessions(
      K key,
      long earliestSessionEndTime,
      long latestSessionStartTime
  ) {
    final var sourceOfTruthIterator = this.sourceOfTruth.findSessions(key, earliestSessionEndTime,
        latestSessionStartTime
    );
    final var candidateIterator = this.candidate.findSessions(key, earliestSessionEndTime,
        latestSessionStartTime
    );

    List<KeyValue<Windowed<K>, V>> expectedResults = new ArrayList<>();
    List<KeyValue<Windowed<K>, V>> actualResults = new ArrayList<>();
    while (sourceOfTruthIterator.hasNext()) {
      expectedResults.add(sourceOfTruthIterator.next());
    }
    while (candidateIterator.hasNext()) {
      actualResults.add(candidateIterator.next());
    }

    this.compare.apply(
        "findSessions", new Object[] {key, earliestSessionEndTime, latestSessionStartTime},
        actualResults.size(), expectedResults.size()
    );
    for (var i = 0; i < actualResults.size(); i++) {
      var actual = actualResults.get(i);
      var expected = expectedResults.get(i);

      this.compare.apply(
          "findSessions", new Object[] {key, earliestSessionEndTime, latestSessionStartTime},
          actual.key.key(), expected.key.key()
      );
      this.compare.apply(
          "findSessions", new Object[] {key, earliestSessionEndTime, latestSessionStartTime},
          actual.value, expected.value
      );
    }
    return Iterators.kv(actualResults.iterator(), el -> el);
  }
}
