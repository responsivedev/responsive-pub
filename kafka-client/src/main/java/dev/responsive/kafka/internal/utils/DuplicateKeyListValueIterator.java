/*
 * Copyright 2024 Responsive Computing, Inc.
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

package dev.responsive.kafka.internal.utils;

import dev.responsive.kafka.internal.db.mongo.WindowDoc;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;

public class DuplicateKeyListValueIterator implements KeyValueIterator<WindowedKey, byte[]> {

  private final Iterator<WindowDoc> remoteResults;
  private final Function<WindowDoc, WindowedKey> keyExtractor;
  private WindowResult currentWindow;

  public DuplicateKeyListValueIterator(
      final Iterator<WindowDoc> remoteResults,
      final Function<WindowDoc, WindowedKey> keyExtractor
  ) {
    this.remoteResults = remoteResults;
    this.keyExtractor = keyExtractor;

    if (remoteResults.hasNext()) {
      final WindowDoc firstDoc = remoteResults.next();
      currentWindow = new WindowResult(firstDoc, keyExtractor);
    }
  }

  @Override
  public void close() {

  }

  @Override
  public WindowedKey peekNextKey() {
    if (currentWindow == null) {
      return null;
    } else {
      return currentWindow.key;
    }
  }

  @Override
  public boolean hasNext() {
    return currentWindow != null;
  }

  @Override
  public KeyValue<WindowedKey, byte[]> next() {
    final KeyValue<WindowedKey, byte[]> next = currentWindow.next();

    if (!currentWindow.hasNext()) {
      if (remoteResults.hasNext()) {
        final WindowDoc nextDoc = remoteResults.next();
        currentWindow = new WindowResult(nextDoc, keyExtractor);
      } else {
        currentWindow = null;
      }
    }

    return next;
  }

  private static class WindowResult {
    private final WindowedKey key;
    private final List<byte[]> values;
    private int valueIndex = 0;

    public WindowResult(
        final WindowDoc windowDoc,
        final Function<WindowDoc, WindowedKey> keyExtractor
    ) {
      this.key = keyExtractor.apply(windowDoc);
      this.values = windowDoc.getValues();
    }

    boolean hasNext() {
      return valueIndex < values.size();
    }

    KeyValue<WindowedKey, byte[]> next() {
      return new KeyValue<>(key, values.get(valueIndex++));
    }
  }
}
