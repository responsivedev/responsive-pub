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

package dev.responsive.kafka.testutils.processors;

import java.util.Collections;
import java.util.Set;
import java.util.function.Function;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.state.StoreBuilder;

public class GenericProcessorSuppliers {

  public static <KIn, VIn, KOut, VOut> ProcessorSupplier<KIn, VIn, KOut, VOut> getSupplier(
      final Function<String, Processor<KIn, VIn, KOut, VOut>> processorForStoreName,
      final StoreBuilder<?> storeBuilder
  ) {
    return new GenericProcessorSupplier<>(processorForStoreName, storeBuilder);
  }

  public static <KIn, VIn, VOut> FixedKeyProcessorSupplier<KIn, VIn, VOut> getFixedKeySupplier(
      final Function<String, FixedKeyProcessor<KIn, VIn, VOut>> processorForStoreName,
      final StoreBuilder<?> storeBuilder
  ) {
    return new GenericFixedKeyProcessorSupplier<>(processorForStoreName, storeBuilder);
  }

  private static class GenericProcessorSupplier<KIn, VIn, KOut, VOut>
      implements ProcessorSupplier<KIn, VIn, KOut, VOut> {

    private final Function<String, Processor<KIn, VIn, KOut, VOut>> processorForStoreName;
    private final StoreBuilder<?> storeBuilder;

    public GenericProcessorSupplier(
        final Function<String, Processor<KIn, VIn, KOut, VOut>> processorForStoreName,
        final StoreBuilder<?> storeBuilder
    ) {
      this.processorForStoreName = processorForStoreName;
      this.storeBuilder = storeBuilder;
    }

    @Override
    public Processor<KIn, VIn, KOut, VOut> get() {
      return processorForStoreName.apply(storeBuilder.name());
    }

    @Override
    public Set<StoreBuilder<?>> stores() {
      if (storeBuilder != null) {
        return Collections.singleton(storeBuilder);
      }
      return null;
    }
  }

  private static class GenericFixedKeyProcessorSupplier<KIn, VIn, VOut>
      implements FixedKeyProcessorSupplier<KIn, VIn, VOut> {

    private final Function<String, FixedKeyProcessor<KIn, VIn, VOut>> processorForStoreName;
    private final StoreBuilder<?> storeBuilder;

    public GenericFixedKeyProcessorSupplier(
        final Function<String, FixedKeyProcessor<KIn, VIn, VOut>> processorForStoreName,
        final StoreBuilder<?> storeBuilder
    ) {
      this.processorForStoreName = processorForStoreName;
      this.storeBuilder = storeBuilder;
    }

    @Override
    public FixedKeyProcessor<KIn, VIn, VOut> get() {
      return processorForStoreName.apply(storeBuilder.name());
    }

    @Override
    public Set<StoreBuilder<?>> stores() {
      if (storeBuilder != null) {
        return Collections.singleton(storeBuilder);
      }
      return null;
    }
  }

}
