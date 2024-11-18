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
