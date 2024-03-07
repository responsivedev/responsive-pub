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

package dev.responsive.kafka.testutils;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.StoreSupplier;

public class StoreComparatorSuppliers {

  @FunctionalInterface
  public interface CompareFunction {
    void apply(String method, Object[] args, Object actual, Object truth);
  }

  public static class MultiKeyValueStoreSupplier implements KeyValueBytesStoreSupplier {
    private final KeyValueBytesStoreSupplier sourceOfTruth;
    private final KeyValueBytesStoreSupplier candidate;
    private final CompareFunction compare;

    public MultiKeyValueStoreSupplier(
        KeyValueBytesStoreSupplier sourceOfTruth,
        KeyValueBytesStoreSupplier candidate
    ) {
      this.sourceOfTruth = sourceOfTruth;
      this.candidate = candidate;
      this.compare = null;
    }

    public MultiKeyValueStoreSupplier(
        KeyValueBytesStoreSupplier sourceOfTruth,
        KeyValueBytesStoreSupplier candidate,
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
    public KeyValueStore<Bytes, byte[]> get() {
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
    private final CompareFunction compare;

    public MultiSessionStoreSupplier(
        StoreSupplier<SessionStore<K, V>> sourceOfTruth,
        StoreSupplier<SessionStore<K, V>> candidate
    ) {
      this.sourceOfTruth = sourceOfTruth;
      this.candidate = candidate;
      this.compare = null;
    }

    public MultiSessionStoreSupplier(
        StoreSupplier<SessionStore<K, V>> sourceOfTruth,
        StoreSupplier<SessionStore<K, V>> candidate,
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
    public SessionStore<K, V> get() {
      if (this.compare == null) {
        return new SessionStoreComparator<>(this.sourceOfTruth.get(), this.candidate.get());
      }
      return new SessionStoreComparator<>(
          this.sourceOfTruth.get(),
          this.candidate.get(),
          this.compare
      );
    }
  }
}
