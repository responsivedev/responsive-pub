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
