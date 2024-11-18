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

import dev.responsive.kafka.internal.stores.ResponsiveSessionStore;
import java.util.Locale;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.SessionBytesStoreSupplier;
import org.apache.kafka.streams.state.SessionStore;

public class ResponsiveSessionBytesStoreSupplier implements SessionBytesStoreSupplier {

  private final ResponsiveSessionParams params;
  private final long segmentIntervalMs;

  public ResponsiveSessionBytesStoreSupplier(final ResponsiveSessionParams params) {
    this.params = params;
    this.segmentIntervalMs = computeSegmentInterval(params.retentionPeriod(), params.numSegments());
  }

  @Override
  public String name() {
    return this.params.name().kafkaName();
  }

  @Override
  public SessionStore<Bytes, byte[]> get() {
    return new ResponsiveSessionStore(this.params);
  }

  @Override
  public long segmentIntervalMs() {
    return segmentIntervalMs;
  }

  @Override
  public long retentionPeriod() {
    return params.retentionPeriod();
  }

  @Override
  public String metricsScope() {
    return "responsive-" + params.schemaType().name().toLowerCase(Locale.ROOT);
  }
}