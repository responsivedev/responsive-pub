package dev.responsive.kafka.api.async.internals.contexts;

import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorContext;

public interface MergedProcessorContext<KOut, VOut>
    extends ProcessorContext<KOut, VOut>, FixedKeyProcessorContext<KOut, VOut> {
}
