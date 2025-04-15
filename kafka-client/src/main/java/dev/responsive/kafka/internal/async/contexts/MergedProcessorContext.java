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

package dev.responsive.kafka.internal.async.contexts;

import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;

/**
 * Simple interface for processor contexts that may be used for both
 * fixed-key and non-fixed-key processors
 */
public interface MergedProcessorContext<KOut, VOut>
    extends InternalProcessorContext<KOut, VOut>, FixedKeyProcessorContext<KOut, VOut> {
}
