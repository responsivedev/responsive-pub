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

package dev.responsive.kafka.api.async.internals;

import static dev.responsive.kafka.api.config.ResponsiveConfig.ASYNC_THREAD_POOL_SIZE_CONFIG;
import static dev.responsive.kafka.api.config.ResponsiveConfig.responsiveConfig;

import dev.responsive.kafka.api.async.internals.stores.AbstractAsyncStoreBuilder;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;

/**
 * This class is in charge of two things:
 * 1. deduplicating and delegating code between the regular and fixed-key processor versions
 * 2. disabling async processing altogether if the thread pool size is 0
 */
public class MaybeAsyncProcessor<KIn, VIn, KOut, VOut>
    implements Processor<KIn, VIn, KOut, VOut>, FixedKeyProcessor<KIn, VIn, VOut> {

  // Exactly one of these is non-null and the other is null
  private final Processor<KIn, VIn, KOut, VOut> userProcessor;
  private final FixedKeyProcessor<KIn, VIn, VOut> userFixedKeyProcessor;

  private final Map<String, AbstractAsyncStoreBuilder<?, ?, ?>> connectedStoreBuilders;

  private Optional<AsyncProcessor<KIn, VIn, KOut, VOut>> asyncProcessor = null;

  public static <KIn, VIn, KOut, VOut> MaybeAsyncProcessor<KIn, VIn, KOut, VOut> createProcessor(
      final Processor<KIn, VIn, KOut, VOut> userProcessor,
      final Map<String, AbstractAsyncStoreBuilder<?, ?, ?>> connectedStoreBuilders
  ) {
    return new MaybeAsyncProcessor<>(userProcessor, null, connectedStoreBuilders);
  }

  public static <KIn, VIn, VOut> MaybeAsyncProcessor<KIn, VIn, KIn, VOut> createFixedKeyProcessor(
      final FixedKeyProcessor<KIn, VIn, VOut> userProcessor,
      final Map<String, AbstractAsyncStoreBuilder<?, ?, ?>> connectedStoreBuilders
  ) {
    return new MaybeAsyncProcessor<>(null, userProcessor, connectedStoreBuilders);
  }

  private MaybeAsyncProcessor(
      final Processor<KIn, VIn, KOut, VOut> userProcessor,
      final FixedKeyProcessor<KIn, VIn, VOut> userFixedKeyProcessor,
      final Map<String, AbstractAsyncStoreBuilder<?, ?, ?>> connectedStoreBuilders
  ) {
    this.userProcessor = userProcessor;
    this.userFixedKeyProcessor = userFixedKeyProcessor;
    this.connectedStoreBuilders = connectedStoreBuilders;

    if (userProcessor == null && userFixedKeyProcessor == null) {
      throw new IllegalStateException("Both the Processor and FixedKeyProcessor were null");
    } else if (userProcessor != null && userFixedKeyProcessor != null) {
      throw new IllegalStateException("Both the Processor and FixedKeyProcessor were non-null");
    }
  }

  @Override
  public void init(final ProcessorContext<KOut, VOut> context) {
    sharedInit(
        (InternalProcessorContext<KOut, VOut>) context,
        () -> userProcessor.init(context)
    );
  }

  @Override
  @SuppressWarnings("unchecked") // needed for fixed-key only since KOut is replaced with KIn
  public void init(final FixedKeyProcessorContext<KIn, VOut> context) {
    sharedInit(
        (InternalProcessorContext<KOut, VOut>) context,
        () -> userFixedKeyProcessor.init(context)
    );
  }

  private void sharedInit(
      final InternalProcessorContext<KOut, VOut> context,
      final Runnable userInit
  ) {
    final int asyncThreadPoolSize =
        responsiveConfig(context.appConfigs()).getInt(ASYNC_THREAD_POOL_SIZE_CONFIG);

    if (asyncThreadPoolSize > 0) {
      this.asyncProcessor = Optional.of(
          new AsyncProcessor<>(connectedStoreBuilders, context, userInit)
      );
    } else {
      this.asyncProcessor = Optional.empty();
      userInit.run();
    }
  }

  @Override
  public void process(final Record<KIn, VIn> record) {
    final Runnable userProcess = () -> userProcessor.process(record);

    if (asyncProcessor.isPresent()) {
      asyncProcessor.get().processRegular(record, userProcess);
    } else {
      userProcess.run();
    }
  }

  @Override
  public void process(final FixedKeyRecord<KIn, VIn> record) {
    final Runnable userProcess = () -> userFixedKeyProcessor.process(record);

    if (asyncProcessor.isPresent()) {
      asyncProcessor.get().processFixedKey(record, userProcess);
    } else {
      userProcess.run();
    }
  }

  @Override
  public void close() {
    final Runnable userClose;
    if (userProcessor != null) {
      userClose = userProcessor::close;
    } else {
      userClose = userFixedKeyProcessor::close;
    }

    if (asyncProcessor.isPresent()) {
      asyncProcessor.get().close(userClose);
    } else {
      userClose.run();
    }
  }
}
