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

package dev.responsive.kafka.testutils;

import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;

@SuppressWarnings("checkstyle:linelength")
public class SimpleStatelessProcessorSupplier<KIn, VIn, VOut> implements FixedKeyProcessorSupplier<KIn, VIn, VOut> {

  private final ComputeStatelessOutput<KIn, VIn, VOut> computeStatelessOutput;

  public SimpleStatelessProcessorSupplier(
      final ComputeStatelessOutput<KIn, VIn, VOut> computeStatelessOutput
  ) {
    this.computeStatelessOutput = computeStatelessOutput;
  }

  @Override
  public FixedKeyProcessor<KIn, VIn, VOut> get() {
    return new SimpleStatelessProcessor<>(computeStatelessOutput);
  }

  @FunctionalInterface
  public interface ComputeStatelessOutput<KIn, VIn, VOut> {
    VOut computeOutput(
        FixedKeyRecord<KIn, VIn> inputRecord,
        FixedKeyProcessorContext<KIn, VOut> context
    );
  }

  public static class SimpleStatelessProcessor<KIn, VIn, VOut> implements FixedKeyProcessor<KIn, VIn, VOut> {

    private final ComputeStatelessOutput<KIn, VIn, VOut> computeStatelessOutput;

    private FixedKeyProcessorContext<KIn, VOut> context;

    public SimpleStatelessProcessor(final ComputeStatelessOutput<KIn, VIn, VOut> computeStatelessOutput) {
      this.computeStatelessOutput = computeStatelessOutput;
    }

    @Override
    public void init(final FixedKeyProcessorContext<KIn, VOut> context) {
      this.context = context;
    }

    @Override
    public void process(final FixedKeyRecord<KIn, VIn> record) {
      final VOut newValue = computeStatelessOutput.computeOutput(record, context);
      context.forward(record.withValue(newValue));
    }
  }
}
