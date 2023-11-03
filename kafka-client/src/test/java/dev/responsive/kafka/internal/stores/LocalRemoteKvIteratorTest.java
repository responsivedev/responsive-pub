/*
 * Copyright 2023 Responsive Computing, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dev.responsive.kafka.internal.stores;

import static org.hamcrest.MatcherAssert.assertThat;

import dev.responsive.kafka.internal.utils.Result;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

public class LocalRemoteKvIteratorTest {

  private static final long TIMESTAMP = 100L;

  @Test
  public void shouldReturnUniqueKeysLexicographicallyFromTwoSources() {
    // Given:
    final byte[] val = new byte[]{0x0};
    final Bytes k1 = Bytes.wrap(new byte[]{0x0});
    final Bytes k2 = Bytes.wrap(new byte[]{0x1});
    final Bytes k3 = Bytes.wrap(new byte[]{0x2});

    final TestKvIterator<Result<Bytes>> buffered = new TestKvIterator<>(
        List.of(k2),
        List.of(Result.value(k2, val, TIMESTAMP))
    );
    final TestKvIterator<byte[]> remote = new TestKvIterator<>(
        List.of(k1, k3),
        List.of(val, val)
    );

    // When:
    final List<Bytes> flushed = new ArrayList<>();
    new LocalRemoteKvIterator<>(buffered, remote, Bytes::compareTo)
        .forEachRemaining(kv -> flushed.add(kv.key));

    // Then:
    assertThat(flushed, Matchers.hasSize(3));
    assertThat(flushed, Matchers.hasItems(k1, k2, k3));
  }

  @Test
  public void shouldReturnLocalOnConflictingKeys() {
    // Given:
    final byte[] valLocal = new byte[]{0x0};
    final byte[] valRemote = new byte[]{0x1};

    final Bytes k1 = Bytes.wrap(new byte[]{0x0});
    final Bytes k2 = Bytes.wrap(new byte[]{0x1});

    final TestKvIterator<Result<Bytes>> buffered = new TestKvIterator<>(
        List.of(k1, k2),
        List.of(Result.value(k1, valLocal, TIMESTAMP), Result.value(k2, valLocal, TIMESTAMP))
    );
    final TestKvIterator<byte[]> remote = new TestKvIterator<>(
        List.of(k1, k2),
        List.of(valRemote, valRemote)
    );

    // When:
    final List<byte[]> flushed = new ArrayList<>();
    new LocalRemoteKvIterator<>(buffered, remote, Bytes::compareTo)
        .forEachRemaining(kv -> flushed.add(kv.value));

    // Then:
    assertThat(flushed, Matchers.hasSize(2));
    assertThat(flushed, Matchers.hasItems(valLocal, valLocal));
  }

  @Test
  public void shouldIgnoreTombstoneWithNoRemoteMatch() {
    // Given:
    final byte[] valRemote = new byte[]{0x1};

    final Bytes k1 = Bytes.wrap(new byte[]{0x0});
    final Bytes k2 = Bytes.wrap(new byte[]{0x1});

    final TestKvIterator<Result<Bytes>> buffered = new TestKvIterator<>(
        List.of(k1),
        List.of(Result.tombstone(k1, TIMESTAMP))
    );
    final TestKvIterator<byte[]> remote = new TestKvIterator<>(
        List.of(k2),
        List.of(valRemote)
    );

    // When:
    final List<Bytes> flushed = new ArrayList<>();
    new LocalRemoteKvIterator<>(buffered, remote, Bytes::compareTo)
        .forEachRemaining(kv -> flushed.add(kv.key));

    // Then:
    assertThat(flushed, Matchers.hasSize(1));
    assertThat(flushed, Matchers.hasItems(k2));
  }

  @Test
  public void shouldIgnoreRemoteValueWithLocalTombstone() {
    // Given:
    final byte[] valRemote = new byte[]{0x1};

    final Bytes k1 = Bytes.wrap(new byte[]{0x0});
    final Bytes k2 = Bytes.wrap(new byte[]{0x1});

    final TestKvIterator<Result<Bytes>> buffered = new TestKvIterator<>(
        List.of(k1),
        List.of(Result.tombstone(k1, TIMESTAMP))
    );
    final TestKvIterator<byte[]> remote = new TestKvIterator<>(
        List.of(k1, k2),
        List.of(valRemote, valRemote)
    );

    // When:
    final List<Bytes> flushed = new ArrayList<>();
    new LocalRemoteKvIterator<>(buffered, remote, Bytes::compareTo)
        .forEachRemaining(kv -> flushed.add(kv.key));

    // Then:
    assertThat(flushed, Matchers.hasSize(1));
    assertThat(flushed, Matchers.hasItems(k2));
  }

  @Test
  public void shouldIterateLocalOnlyWithTombstones() {
    // Given:
    final byte[] val = new byte[]{0x0};
    final Bytes k1 = Bytes.wrap(new byte[]{0x0});
    final Bytes k2 = Bytes.wrap(new byte[]{0x1});
    final Bytes k3 = Bytes.wrap(new byte[]{0x2});

    final TestKvIterator<Result<Bytes>> buffered = new TestKvIterator<>(
        List.of(k1, k2, k3),
        List.of(
            Result.value(k1, val, TIMESTAMP),
            Result.tombstone(k2, TIMESTAMP),
            Result.value(k3, val, TIMESTAMP))
    );
    final TestKvIterator<byte[]> remote = new TestKvIterator<>(List.of(), List.of());

    // When:
    final List<Bytes> flushed = new ArrayList<>();
    new LocalRemoteKvIterator<>(buffered, remote, Bytes::compareTo)
        .forEachRemaining(kv -> flushed.add(kv.key));

    // Then:
    assertThat(flushed, Matchers.hasSize(2));
    assertThat(flushed, Matchers.hasItems(k1, k3));
  }

  @Test
  public void shouldIterateRemoteOnly() {
    // Given:
    final byte[] val = new byte[]{0x0};
    final Bytes k1 = Bytes.wrap(new byte[]{0x0});
    final Bytes k2 = Bytes.wrap(new byte[]{0x1});
    final Bytes k3 = Bytes.wrap(new byte[]{0x2});

    final TestKvIterator<Result<Bytes>> buffered = new TestKvIterator<>(List.of(), List.of());
    final TestKvIterator<byte[]> remote = new TestKvIterator<>(
        List.of(k1, k2, k3),
        List.of(val, val, val)
    );

    // When:
    final List<Bytes> flushed = new ArrayList<>();
    new LocalRemoteKvIterator<>(buffered, remote, Bytes::compareTo)
        .forEachRemaining(kv -> flushed.add(kv.key));

    // Then:
    assertThat(flushed, Matchers.hasSize(3));
    assertThat(flushed, Matchers.hasItems(k1, k2, k3));
  }

  private static class TestKvIterator<T> implements KeyValueIterator<Bytes, T> {

    private final List<Bytes> keys;
    private final List<T> val;
    private int idx = 0;

    public TestKvIterator(final List<Bytes> keys, final List<T> val) {
      if (keys.size() != val.size()) {
        throw new IllegalArgumentException();
      }
      this.keys = keys;
      this.val = val;
    }

    @Override
    public void close() {
    }

    @Override
    public Bytes peekNextKey() {
      return keys.get(idx);
    }

    @Override
    public boolean hasNext() {
      return idx < keys.size();
    }

    @Override
    public KeyValue<Bytes, T> next() {
      return new KeyValue<>(keys.get(idx), val.get(idx++));
    }
  }

}