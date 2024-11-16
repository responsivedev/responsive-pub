/*
 * Copyright 2024 Responsive Computing, Inc.
 *
 * This source code is licensed under the Responsive Software License Agreement v1.0 available at
 *
 * https://www.responsive.dev/legal/responsive-bsl-10
 *
 * This software requires a valid Commercial License Key for production use. Trial and commercial
 * licenses can be obtained at https://www.responsive.dev/sdk/get-started.
 */

package dev.responsive.kafka.internal.db.partitioning;

import java.util.function.Function;
import org.apache.kafka.common.utils.Bytes;

/**
 * An interface that can be implemented to customize the
 * hashing logic for {@link SubPartitioner}.
 */
public interface Hasher extends Function<Bytes, Integer> {

}
