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

package dev.responsive.kafka.internal.db;

import dev.responsive.kafka.internal.stores.RemoteWriteResult;
import java.util.concurrent.CompletionStage;

/**
 * An instance of a writer for a single remote table partition
 *
 * @param <K> the data key type
 * @param <P> the table partition type
 */
public interface RemoteWriter<K, P> {

  void insert(final K key, final byte[] value, long timestampMs);

  void delete(final K key);

  CompletionStage<RemoteWriteResult<P>> flush();

}
