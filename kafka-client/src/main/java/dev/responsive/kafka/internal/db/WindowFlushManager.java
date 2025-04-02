/*
 * Copyright 2025 Responsive Computing, Inc.
 *
 * This source code is licensed under the Responsive Business Source License Agreement v1.0
 * available at:
 *
 * https://www.responsive.dev/legal/responsive-bsl-10
 *
 * This software requires a valid Commercial License Key for production use. Trial and commercial
 * licenses can be obtained at https://www.responsive.dev
 */

package dev.responsive.kafka.internal.db;

import dev.responsive.kafka.internal.utils.WindowedKey;

/**
 *
 * @param <P> Table partition type
 */
public interface WindowFlushManager<P> extends FlushManager<WindowedKey, P> {

  long streamTime();

}
