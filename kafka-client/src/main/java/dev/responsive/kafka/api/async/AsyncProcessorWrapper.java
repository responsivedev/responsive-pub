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

import org.apache.kafka.streams.ProcessorWrapper;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;

public class AsyncProcessorWrapper implements ProcessorWrapper {


  @Override
  public <KIn, VIn, KOut, VOut> ProcessorSupplier<KIn, VIn, KOut, VOut> wrapProcessorSupplier(
      final String processorName,
      final ProcessorSupplier<KIn, VIn, KOut, VOut> processorSupplier
  ) {
    return AsyncProcessorSupplier.createAsyncProcessorSupplier(processorSupplier);
  }

  @Override
  public <KIn, VIn, VOut> FixedKeyProcessorSupplier<KIn, VIn, VOut> wrapFixedKeyProcessorSupplier(
      final String processorName,
      final FixedKeyProcessorSupplier<KIn, VIn, VOut> fixedKeyProcessorSupplier
  ) {
    return AsyncFixedKeyProcessorSupplier.createAsyncProcessorSupplier(fixedKeyProcessorSupplier);
  }
}
