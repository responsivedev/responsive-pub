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

package dev.responsive.kafka.api.async;

import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.ProcessorWrapper;
import org.apache.kafka.streams.processor.api.WrappedFixedKeyProcessorSupplier;
import org.apache.kafka.streams.processor.api.WrappedProcessorSupplier;

// TODO(sophie): only wrap when stores are Responsive (rather than eg rocksdb or in-memory)
public class AsyncProcessorWrapper implements ProcessorWrapper {

  @Override
  public <KIn, VIn, KOut, VOut> WrappedProcessorSupplier<KIn, VIn, KOut, VOut> wrapProcessorSupplier(
      final String processorName,
      final ProcessorSupplier<KIn, VIn, KOut, VOut> processorSupplier
  ) {
    final var stores = processorSupplier.stores();
    if (stores != null && !stores.isEmpty()) {
      return AsyncProcessorSupplier.createAsyncProcessorSupplier(processorSupplier);
    } else {
      return ProcessorWrapper.asWrapped(processorSupplier);
    }
  }

  @Override
  public <KIn, VIn, VOut> WrappedFixedKeyProcessorSupplier<KIn, VIn, VOut> wrapFixedKeyProcessorSupplier(
      final String processorName,
      final FixedKeyProcessorSupplier<KIn, VIn, VOut> processorSupplier
  ) {
    final var stores = processorSupplier.stores();
    if (stores != null && !stores.isEmpty()) {
      return AsyncFixedKeyProcessorSupplier.createAsyncProcessorSupplier(processorSupplier);
    } else {
      return ProcessorWrapper.asWrappedFixedKey(processorSupplier);
    }

  }
}
