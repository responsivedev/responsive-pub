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
