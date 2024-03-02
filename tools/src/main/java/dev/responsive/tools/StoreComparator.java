/*
 *  Copyright 2024 Responsive Computing, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package dev.responsive.tools;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.CommitCallback;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.StoreSupplier;

public class StoreComparator<K, V> {

  public static class SessionStoreComparator<K, V> implements SessionStore<K, V> {
    private final SessionStore<K, V> sourceOfTruth;
    private final SessionStore<K, V> candidate;
    private final FailureFunction onFailure;

    public SessionStoreComparator(SessionStore<K, V> sourceOfTruth, SessionStore<K, V> candidate) {
      this(
          sourceOfTruth,
          candidate,
          (String reason, String method, Object[] args, Object truth, Object actual) -> {
            System.out.printf("StoreComparator failure: %s | %s | %s", reason, method, args);
          }
      );
    }

    public SessionStoreComparator(
        SessionStore<K, V> sourceOfTruth, SessionStore<K, V> candidate,
        FailureFunction onFailure
    ) {
      this.sourceOfTruth = sourceOfTruth;
      this.candidate = candidate;
      this.onFailure = onFailure;
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
      assertEqual(
          "isOpen should match for both stores",
          "isOpen", new Object[] {},
          this.candidate.isOpen(), this.sourceOfTruth.isOpen()
      );
      return this.candidate.isOpen();
    }

    @Override
    public Position getPosition() {
      assertEqual(
          "getPosition should return the same position for both stores",
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

      assertEqual(
          "fetch results from both stores should have the same count",
          "fetch", new Object[] {bytes},
          actualResults.size(), expectedResults.size()
      );
      for (var i = 0; i < actualResults.size(); i++) {
        var actual = actualResults.get(i);
        var expected = expectedResults.get(i);

        assertEqual(
            "fetch result keys should be identical for both stores",
            "fetch", new Object[] {bytes},
            actual.key.key(), expected.key.key()
        );
        assertEqual(
            "fetch result values should be identical for both stores",
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

      assertEqual(
          "fetch results from both stores should have the same count",
          "fetch", new Object[] {bytes, k1},
          actualResults.size(), expectedResults.size()
      );
      for (var i = 0; i < actualResults.size(); i++) {
        var actual = actualResults.get(i);
        var expected = expectedResults.get(i);

        assertEqual(
            "fetch result keys should be identical for both stores",
            "fetch", new Object[] {bytes, k1},
            actual.key.key(), expected.key.key()
        );
        assertEqual(
            "fetch result values should be identical for both stores",
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
      assertEqual(
          "results from both stores should be identical",
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

      assertEqual(
          "results from both stores should have the same count",
          "findSessions", new Object[] {key, earliestSessionEndTime, latestSessionStartTime},
          actualResults.size(), expectedResults.size()
      );
      for (var i = 0; i < actualResults.size(); i++) {
        var actual = actualResults.get(i);
        var expected = expectedResults.get(i);

        assertEqual(
            "result keys should be identical for both stores",
            "findSessions", new Object[] {key, earliestSessionEndTime, latestSessionStartTime},
            actual.key.key(), expected.key.key()
        );
        assertEqual(
            "result values should be identical for both stores",
            "findSessions", new Object[] {key, earliestSessionEndTime, latestSessionStartTime},
            actual.value, expected.value
        );
      }
      return Iterators.kv(actualResults.iterator(), el -> el);
    }

    private void assertEqual(
        String reason, String method, Object[] args,
        Object actual, Object expected
    ) {
      if (actual.equals(expected)) {
        return;
      }
      this.onFailure.apply(reason, method, args, actual, expected);
    }
  }

  @FunctionalInterface
  public interface FailureFunction {
    void apply(String reason, String method, Object[] args, Object actual, Object truth);
  }

  public static class MultiSessionStoreSupplier<K, V> implements StoreSupplier<SessionStore<K, V>> {
    private final StoreSupplier<SessionStore<K, V>> sourceOfTruth;
    private final StoreSupplier<SessionStore<K, V>> candidate;
    private final FailureFunction onFailure;

    public MultiSessionStoreSupplier(
        StoreSupplier<SessionStore<K, V>> sourceOfTruth,
        StoreSupplier<SessionStore<K, V>> candidate
    ) {
      this.sourceOfTruth = sourceOfTruth;
      this.candidate = candidate;
      this.onFailure = null;
    }

    public MultiSessionStoreSupplier(
        StoreSupplier<SessionStore<K, V>> sourceOfTruth,
        StoreSupplier<SessionStore<K, V>> candidate,
        FailureFunction onFailure
    ) {
      this.sourceOfTruth = sourceOfTruth;
      this.candidate = candidate;
      this.onFailure = onFailure;
    }

    @Override
    public String metricsScope() {
      return this.sourceOfTruth.metricsScope();
    }

    @Override
    public String name() {
      return this.sourceOfTruth.name();
    }

    @Override
    public SessionStore<K, V> get() {
      if (this.onFailure == null) {
        return new SessionStoreComparator<>(this.sourceOfTruth.get(), this.candidate.get());
      }
      return new SessionStoreComparator<>(
          this.sourceOfTruth.get(),
          this.candidate.get(),
          this.onFailure
      );
    }
  }

  private static class MultiStateStoreContext implements InvocationHandler, StateRestoreCallback,
      CommitCallback {
    private final StateStoreContext delegate;
    private final StateStore root;

    private final List<StateRestoreCallback> restoreCallbacks;
    private final List<CommitCallback> commitCallbacks;

    private boolean registered;

    public MultiStateStoreContext(StateStoreContext delegate, StateStore root) {
      this.delegate = delegate;
      this.root = root;

      this.registered = false;
      this.restoreCallbacks = new ArrayList<>();
      this.commitCallbacks = new ArrayList<>();
    }

    @Override
    public Object invoke(final Object proxy, final Method method, final Object[] args)
        throws Throwable {
      if (method.getName().equals("register")) {
        if (args.length == 2) {
          this.register(args[0], args[1]);
          return null;
        } else if (args.length == 3) {
          this.register(args[0], args[1], args[2]);
          return null;
        } else {
          throw new RuntimeException("Failed to proxy register call.");
        }
      }
      return method.invoke(this.delegate, args);
    }

    public void register(
        final Object stateStore,
        final Object stateRestoreCallback
    ) {
      this.register((StateStore) stateStore, (StateRestoreCallback) stateRestoreCallback);
    }

    public void register(
        final Object stateStore,
        final Object stateRestoreCallback,
        final Object commitCallback
    ) {
      this.register((StateStore) stateStore, (StateRestoreCallback) stateRestoreCallback,
          (CommitCallback) commitCallback);
    }

    public void register(
        final StateStore stateStore,
        final StateRestoreCallback stateRestoreCallback
    ) {
      this.restoreCallbacks.add(stateRestoreCallback);

      if (!this.registered) {
        this.delegate.register(this.root, this, this);
        this.registered = true;
      }
    }

    public void register(
        final StateStore stateStore,
        final StateRestoreCallback stateRestoreCallback,
        final CommitCallback commitCallback
    ) {
      this.restoreCallbacks.add(stateRestoreCallback);
      this.commitCallbacks.add(commitCallback);

      if (!this.registered) {
        this.delegate.register(this.root, this, this);
        this.registered = true;
      }
    }

    @Override
    public void restore(final byte[] bytes, final byte[] bytes1) {
      this.restoreCallbacks.forEach(callback -> callback.restore(bytes, bytes1));
    }

    @Override
    public void onCommit() throws IOException {
      for (var callback : this.commitCallbacks) {
        callback.onCommit();
      }
    }
  }
}
