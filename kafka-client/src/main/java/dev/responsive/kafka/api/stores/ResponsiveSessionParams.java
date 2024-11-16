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

package dev.responsive.kafka.api.stores;

import static dev.responsive.kafka.internal.utils.StoreUtil.durationToMillis;

import dev.responsive.kafka.internal.stores.SchemaTypes.SessionSchema;
import dev.responsive.kafka.internal.utils.TableName;
import java.time.Duration;

public final class ResponsiveSessionParams {

  private static final long DEFAULT_NUM_SEGMENTS = 10L; // space amplification = 1.1x

  private final TableName name;
  private final SessionSchema schemaType;
  private final long retentionPeriodMs;
  private final long numSegments;

  private ResponsiveSessionParams(
      final String name,
      final SessionSchema schemaType,
      final long retentionPeriodMs
  ) {
    this.name = new TableName(name);
    this.schemaType = schemaType;
    this.retentionPeriodMs = retentionPeriodMs;
    this.numSegments = computeDefaultNumSegments(retentionPeriodMs);
  }

  public static ResponsiveSessionParams session(
      final String name,
      final Duration retention
  ) {
    return new ResponsiveSessionParams(name, SessionSchema.SESSION, retention.toMillis());
  }

  public static ResponsiveSessionParams session(
      final String name,
      final Duration inactivityGap,
      final Duration gracePeriod
  ) {
    final long inactivityGapMs = durationToMillis(inactivityGap, "inactivityGap");
    final long gracePeriodMs = durationToMillis(gracePeriod, "gracePeriod");
    final long retentionPeriodMs = inactivityGapMs + gracePeriodMs;
    return new ResponsiveSessionParams(name, SessionSchema.SESSION, retentionPeriodMs);
  }

  public SessionSchema schemaType() {
    return this.schemaType;
  }

  public TableName name() {
    return this.name;
  }

  public long retentionPeriod() {
    return this.retentionPeriodMs;
  }

  public long numSegments() {
    return this.numSegments;
  }

  private static long computeDefaultNumSegments(final long retentionPeriodMs) {
    // TODO: Smart implementation.
    return DEFAULT_NUM_SEGMENTS;
  }
}
