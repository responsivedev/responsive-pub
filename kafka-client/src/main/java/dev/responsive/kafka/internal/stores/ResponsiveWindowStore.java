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

import static dev.responsive.kafka.api.config.ResponsiveConfig.WINDOW_BLOOM_FILTER_COUNT_CONFIG;
import static dev.responsive.kafka.api.config.ResponsiveConfig.WINDOW_BLOOM_FILTER_EXPECTED_KEYS_CONFIG;
import static dev.responsive.kafka.api.config.ResponsiveConfig.WINDOW_BLOOM_FILTER_FPP_CONFIG;
import static dev.responsive.kafka.internal.db.partitioning.Segmenter.UNINITIALIZED_STREAM_TIME;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import dev.responsive.kafka.api.config.ResponsiveConfig;
import dev.responsive.kafka.api.stores.ResponsiveWindowParams;
import dev.responsive.kafka.internal.utils.Iterators;
import dev.responsive.kafka.internal.utils.TableName;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.apache.kafka.streams.state.internals.StoreQueryUtils;
import org.slf4j.Logger;

public class ResponsiveWindowStore implements WindowStore<Bytes, byte[]> {
  private final Logger log;

  private final ResponsiveWindowParams params;
  private final TableName name;
  private final long retentionPeriod;

  private long initialStreamTime;
  private int numBloomFilterWindows;
  private double fpp;
  private long expectedKeysPerWindow;
  private BloomFilter<byte[]> bloomFilter;

  private Position position; // TODO(IQ): update the position during restoration
  private boolean open;
  private long observedStreamTime = UNINITIALIZED_STREAM_TIME;

  // All the fields below this are effectively final, we just can't set them until #init is called
  private WindowOperations windowOperations;
  private StateStoreContext context;

  public ResponsiveWindowStore(final ResponsiveWindowParams params) {
    this.params = params;
    this.name = params.name();
    this.retentionPeriod = params.retentionPeriod();
    this.position = Position.emptyPosition();
    this.log = new LogContext(
        String.format("window-store [%s] ", name.kafkaName())
    ).logger(ResponsiveWindowStore.class);
  }

  @Override
  public String name() {
    return name.kafkaName();
  }

  @Override
  @Deprecated
  public void init(final ProcessorContext context, final StateStore root) {
    if (context instanceof StateStoreContext) {
      init((StateStoreContext) context, root);
    } else {
      throw new UnsupportedOperationException(
          "Use ResponsiveWindowStore#init(StateStoreContext, StateStore) instead."
      );
    }
  }

  @Override
  public void init(final StateStoreContext storeContext, final StateStore root) {
    try {
      log.info("Initializing state store");

      final var appConfigs = storeContext.appConfigs();
      final ResponsiveConfig config = ResponsiveConfig.responsiveConfig(appConfigs);

      context = storeContext;
      windowOperations = SegmentedOperations.create(
          name,
          storeContext,
          params,
          appConfigs,
          config,
          window -> window.windowStartMs >= minValidTimestamp()
      );

      numBloomFilterWindows = config.getInt(WINDOW_BLOOM_FILTER_COUNT_CONFIG);
      expectedKeysPerWindow = config.getLong(WINDOW_BLOOM_FILTER_EXPECTED_KEYS_CONFIG);
      fpp = config.getDouble(WINDOW_BLOOM_FILTER_FPP_CONFIG);
      initialStreamTime = windowOperations.initialStreamTime();

      log.info("Completed initializing state store");

      open = true;
      storeContext.register(root, windowOperations);
    } catch (InterruptedException | TimeoutException e) {
      throw new ProcessorStateException("Failed to initialize store.", e);
    }
  }

  private boolean inLatestWindowBloomFilter(final long windowStartTime) {
    return hasActiveBloomFilter() && windowStartTime == observedStreamTime;
  }

  private boolean hasActiveBloomFilter() {
    return bloomFilter != null;
  }

  @Override
  public boolean persistent() {
    // Kafka Streams uses this to determine whether it
    // needs to create and lock state directories. since
    // the Responsive Client doesn't require flushing state
    // to disk, we return false even though the store is
    // persistent in a remote store
    return false;
  }

  @Override
  public boolean isOpen() {
    return open;
  }

  @Override
  public void put(final Bytes key, final byte[] value, final long windowStartTime) {
    if (value == null) {
      windowOperations.delete(key, windowStartTime);
    } else {
      windowOperations.put(key, value, windowStartTime);

      if (numBloomFilterWindows > 0) {

        // don't create a bloom filter for the latest window after a restart, since we may be
        // missing some of the data that was inserted into the window prior to the restart
        final boolean shouldRollBloomFilter =
            windowStartTime > observedStreamTime && windowStartTime != initialStreamTime;

        if (shouldRollBloomFilter) {
          createNewBloomFilter(windowStartTime);
        }

        if (shouldRollBloomFilter || inLatestWindowBloomFilter(windowStartTime)) {
          bloomFilter.put(key.get());
        }
      }
    }

    observedStreamTime = Math.max(observedStreamTime, windowStartTime);
    StoreQueryUtils.updatePosition(position, context);
  }

  private void createNewBloomFilter(final long windowStartTime) {
    if (!hasActiveBloomFilter()) {
      log.info("Creating the first bloom filter for window@{} with previous window@{}",
               windowStartTime, observedStreamTime);
    } else {
      final double actualFpp = bloomFilter.expectedFpp();
      final long approxElementCount = bloomFilter.approximateElementCount();
      log.info("Rolling new bloom filter for window@{}, previous filter for window@{} "
                   + "had approx {} elements with estimated fpp={}",
               windowStartTime, observedStreamTime, approxElementCount, actualFpp);

      // TODO(sophie): consider adapting the numKeysPerWindow estimate based on the approx.
      //  count of the last window. According to the #approximateElementCount docs, "This
      //  approximation is reasonably accurate if it does not exceed the value of
      //  {@code expectedInsertions} that was used when constructing the filter".
      //  We can test whether #expectedFpp is close to or smaller than the provided fpp as
      //  an indicator of the count approximation's accuracy, since an #expectedFpp that is
      //  "significantly higher" than the provided fpp signals that the actual number of
      //  elements exceeded the provided expectedInsertions.
      //  If the #expectedFpp indicates we can't trust the count approximation, we know to try
      //  something higher than the previous expectedInsertions value.
      //  Otherwise, we can just use the result of #approximateElementCount
      if (actualFpp > fpp) {
        log.warn("Actual fpp was {} which is greater than requested fpp {}. It's likely that "
                     + "the actual number of elements exceeded the expected keys per window {}",
                 actualFpp, fpp, expectedKeysPerWindow);
      }
    }

    bloomFilter = BloomFilter.create(Funnels.byteArrayFunnel(), expectedKeysPerWindow, fpp);
  }

  @Override
  public byte[] fetch(final Bytes key, final long windowStartTime) {
    if (windowStartTime < minValidTimestamp()) {
      return null;
    }

    if (inLatestWindowBloomFilter(windowStartTime)) {
      return bloomFilter.mightContain(key.get())
          ? windowOperations.fetch(key, windowStartTime)
          : null;
    } else {
      return windowOperations.fetch(key, windowStartTime);
    }
  }

  @Override
  public WindowStoreIterator<byte[]> fetch(
      final Bytes key,
      final long timeFrom,
      final long timeTo
  ) {
    final long minValidTime = minValidTimestamp();

    if (timeTo < minValidTime) {
      return Iterators.windowed(Iterators.emptyKv());
    }

    final long boundedTimeFrom = Math.max(minValidTime, timeFrom);
    return windowOperations.fetch(key, boundedTimeFrom, timeTo);
  }

  @Override
  public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(
      final Bytes keyFrom,
      final Bytes keyTo,
      final long timeFrom,
      final long timeTo
  ) {
    final long minValidTime = minValidTimestamp();

    if (timeTo < minValidTime) {
      return Iterators.emptyKv();
    }

    final long actualTimeFrom = Math.max(minValidTime, timeFrom);
    return windowOperations.fetch(keyFrom, keyTo, actualTimeFrom, timeTo);
  }

  @Override
  public KeyValueIterator<Windowed<Bytes>, byte[]> fetchAll(
      final long timeFrom,
      final long timeTo
  ) {
    final long minValidTime = minValidTimestamp();

    if (timeTo < minValidTime) {
      return Iterators.emptyKv();
    }

    final long actualTimeFrom = Math.max(minValidTime, timeFrom);
    return windowOperations.fetchAll(actualTimeFrom, timeTo);
  }

  @Override
  public KeyValueIterator<Windowed<Bytes>, byte[]> all() {
    return windowOperations.all();
  }

  @Override
  public WindowStoreIterator<byte[]> backwardFetch(
      final Bytes key,
      final long timeFrom,
      final long timeTo
  ) {
    final long minValidTime = minValidTimestamp();

    if (timeTo < minValidTime) {
      return Iterators.windowed(Iterators.emptyKv());
    }

    final long actualTimeFrom = Math.max(minValidTime, timeFrom);
    return windowOperations.backwardFetch(key, actualTimeFrom, timeTo);
  }

  @Override
  public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetch(
      final Bytes keyFrom,
      final Bytes keyTo,
      final long timeFrom,
      final long timeTo
  ) {
    final long minValidTime = minValidTimestamp();

    if (timeTo < minValidTime) {
      return Iterators.emptyKv();
    }

    final long actualTimeFrom = Math.max(minValidTime, timeFrom);
    return windowOperations.backwardFetch(keyFrom, keyTo, actualTimeFrom, timeTo);
  }

  @Override
  public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetchAll(
      final long timeFrom,
      final long timeTo
  ) {
    final long minValidTime = minValidTimestamp();

    if (timeTo < minValidTime) {
      return Iterators.emptyKv();
    }

    final long actualTimeFrom = Math.max(minValidTime, timeFrom);
    return windowOperations.backwardFetchAll(actualTimeFrom, timeTo);
  }

  @Override
  public KeyValueIterator<Windowed<Bytes>, byte[]> backwardAll() {
    return windowOperations.backwardAll();
  }

  @Override
  public void flush() {
  }

  @Override
  public void close() {
    windowOperations.close();
  }

  @Override
  public Position getPosition() {
    return position;
  }

  private long minValidTimestamp() {
    // add one b/c records expire exactly {retentionPeriod}ms after created
    return observedStreamTime - retentionPeriod + 1;
  }

}
