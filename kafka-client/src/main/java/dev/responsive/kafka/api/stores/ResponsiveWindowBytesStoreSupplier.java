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

package dev.responsive.kafka.api.stores;

import static dev.responsive.kafka.internal.utils.StoreUtil.computeSegmentInterval;

import dev.responsive.kafka.internal.stores.ResponsiveWindowStore;
import java.util.Locale;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowStore;

public class ResponsiveWindowBytesStoreSupplier implements WindowBytesStoreSupplier {

  private final ResponsiveWindowParams params;
  private final long segmentIntervalMs;

  public ResponsiveWindowBytesStoreSupplier(final ResponsiveWindowParams params) {
    this.params = params;
    this.segmentIntervalMs = computeSegmentInterval(
        params.retentionPeriodMs(),
        params.numSegments()
    );
  }

  @Override
  public String name() {
    return params.name().kafkaName();
  }

  @Override
  public WindowStore<Bytes, byte[]> get() {
    return new ResponsiveWindowStore(params);
  }

  @Override
  public long segmentIntervalMs() {
    return segmentIntervalMs;
  }

  @Override
  public long windowSize() {
    return params.windowSize();
  }

  @Override
  public boolean retainDuplicates() {
    return params.retainDuplicates();
  }

  @Override
  public long retentionPeriod() {
    return params.retentionPeriodMs();
  }

  @Override
  public String metricsScope() {
    return "responsive-" + params.schemaType().name().toLowerCase(Locale.ROOT);
  }
}