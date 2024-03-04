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
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.streams.processor.CommitCallback;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.StoreSupplier;

public class StoreComparator {

  @FunctionalInterface
  public interface FailureFunction {
    void apply(String reason, String method, Object[] args, Object actual, Object truth);
  }

  @FunctionalInterface
  public interface CompareFunction {
    void apply(String method, Object[] args, Object actual, Object truth);
  }

  public static class MultiKeyValueStoreSupplier<K, V>
      implements StoreSupplier<KeyValueStore<K, V>> {
    private final StoreSupplier<KeyValueStore<K, V>> sourceOfTruth;
    private final StoreSupplier<KeyValueStore<K, V>> candidate;
    private final CompareFunction compare;

    public MultiKeyValueStoreSupplier(
        StoreSupplier<KeyValueStore<K, V>> sourceOfTruth,
        StoreSupplier<KeyValueStore<K, V>> candidate
    ) {
      this.sourceOfTruth = sourceOfTruth;
      this.candidate = candidate;
      this.compare = null;
    }

    public MultiKeyValueStoreSupplier(
        StoreSupplier<KeyValueStore<K, V>> sourceOfTruth,
        StoreSupplier<KeyValueStore<K, V>> candidate,
        CompareFunction compare
    ) {
      this.sourceOfTruth = sourceOfTruth;
      this.candidate = candidate;
      this.compare = compare;
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
    public KeyValueStore<K, V> get() {
      if (this.compare == null) {
        return new KeyValueStoreComparator<>(this.sourceOfTruth.get(), this.candidate.get());
      }
      return new KeyValueStoreComparator<>(
          this.sourceOfTruth.get(),
          this.candidate.get(),
          this.compare
      );
    }
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

  public static class MultiStateStoreContext implements InvocationHandler, StateRestoreCallback,
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
