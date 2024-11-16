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

package dev.responsive.k8s.crd.kafkastreams;

import static java.util.Optional.empty;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import dev.responsive.k8s.crd.CrdUtils;
import java.util.Objects;
import java.util.Optional;

/**
 * <p>Models the spec fo a single diagnoser. A diagnoser defines one or more goals. When
 * the diagnoser is applied in the controller, it monitors the application to detect when
 * the application is not meeting, or likely will soon fail to meet the diagnoser's
 * goals. When this happens, the diagnoser determines why and prescribes an action
 * (e.g. scaling or balancing) to correct the application.</p>
 * <p></p>
 * <p>We currently model diagnoser specs using this class to encapsulate all diagnoser
 * types. A given instance of DiagnoserSpec represents one of the actual DiagnoserSpec types
 * (e.g. ExpectedLatencyDiagnoserSpec or ProcessingRateDiagnoserSpec). The type represented
 * is specified in the type field of this class. It would almost certainly be better
 * to model this in the class hierarchy by having the actual diagnoser spec types inherit
 * from a base type. Unfortunately we cannot do this as the fabric8 CRD generator is
 * not able to generate the policy CRDs from a class model that uses inheritance.</p>
 */
public class DiagnoserSpec {
  public enum Type {
    PROCESSING_RATE_SCALE_UP,
    PROCESSING_RATE_SCALE_DOWN,
    ARRIVAL_RATE_SCALE_UP,
    LAG_SCALE_UP,
    EXPECTED_LATENCY,
    THREAD_SATURATION
  }

  private final Type type;
  private final Optional<RateBasedDiagnoserSpec> processingRateScaleUp;
  private final Optional<RateBasedDiagnoserSpec> processingRateScaleDown;
  private final Optional<RateBasedDiagnoserSpec> arrivalRateScaleUp;
  private final Optional<ExpectedLatencyDiagnoserSpec> expectedLatency;
  private final Optional<ThreadSaturationDiagnoserSpec> threadSaturation;

  @JsonCreator
  public DiagnoserSpec(
      @JsonProperty("type") final Type strategy,
      @JsonProperty("processingRateScaleUp")
      final Optional<RateBasedDiagnoserSpec> processingRateScaleUp,
      @JsonProperty("processingRateScaleDown")
      final Optional<RateBasedDiagnoserSpec> processingRateScaleDown,
      @JsonProperty("arrivalRateScaleUp")
      final Optional<RateBasedDiagnoserSpec> arrivalRateScaleUp,
      @JsonProperty("expectedLatency")
      final Optional<ExpectedLatencyDiagnoserSpec> expectedLatency,
      @JsonProperty("threadSaturation")
      final Optional<ThreadSaturationDiagnoserSpec> threadSaturation
  ) {
    this.type = strategy;
    this.processingRateScaleDown = Objects.requireNonNull(processingRateScaleDown);
    this.processingRateScaleUp = Objects.requireNonNull(processingRateScaleUp);
    this.arrivalRateScaleUp = Objects.requireNonNull(arrivalRateScaleUp);
    this.expectedLatency = Objects.requireNonNull(expectedLatency);
    this.threadSaturation = threadSaturation;
  }

  public static DiagnoserSpec lag() {
    return new DiagnoserSpec(
        Type.LAG_SCALE_UP,
        empty(),
        empty(),
        empty(),
        empty(),
        empty()
    );
  }

  public static DiagnoserSpec processingRateScaleUp(final RateBasedDiagnoserSpec diagnoser) {
    return new DiagnoserSpec(
        Type.PROCESSING_RATE_SCALE_UP,
        Optional.of(diagnoser),
        empty(),
        empty(),
        empty(),
        empty()
    );
  }

  public static DiagnoserSpec processingRateScaleDown(final RateBasedDiagnoserSpec diagnoser) {
    return new DiagnoserSpec(
        Type.PROCESSING_RATE_SCALE_DOWN,
        empty(),
        Optional.of(diagnoser),
        empty(),
        empty(),
        empty()
    );
  }

  public static DiagnoserSpec arrivalRateScaleUp(final RateBasedDiagnoserSpec diagnoser) {
    return new DiagnoserSpec(
        Type.ARRIVAL_RATE_SCALE_UP,
        empty(),
        empty(),
        Optional.of(diagnoser),
        empty(),
        empty()
    );
  }

  public static DiagnoserSpec expectedLatency(final ExpectedLatencyDiagnoserSpec diagnoser) {
    return new DiagnoserSpec(
        Type.EXPECTED_LATENCY,
        empty(),
        empty(),
        empty(),
        Optional.of(diagnoser),
        empty()
    );
  }

  public static DiagnoserSpec thredSaturation(final ThreadSaturationDiagnoserSpec diagnoser) {
    return new DiagnoserSpec(
        Type.THREAD_SATURATION,
        empty(),
        empty(),
        empty(),
        empty(),
        Optional.of(diagnoser)
    );
  }

  public Type getType() {
    return type;
  }

  public Optional<RateBasedDiagnoserSpec> getProcessingRateScaleUp() {
    return processingRateScaleUp;
  }

  public Optional<RateBasedDiagnoserSpec> getProcessingRateScaleDown() {
    return processingRateScaleDown;
  }

  public Optional<ExpectedLatencyDiagnoserSpec> getExpectedLatency() {
    return expectedLatency;
  }

  public Optional<RateBasedDiagnoserSpec> getArrivalRateScaleUp() {
    return arrivalRateScaleUp;
  }

  public Optional<ThreadSaturationDiagnoserSpec> getThreadSaturation() {
    return threadSaturation;
  }

  public void validate() {
    Objects.requireNonNull(type);
    switch (type) {
      case PROCESSING_RATE_SCALE_UP:
        CrdUtils.validatePresent(processingRateScaleUp, "processingRateScaleUp");
        break;
      case PROCESSING_RATE_SCALE_DOWN:
        CrdUtils.validatePresent(processingRateScaleDown, "processingRateScaleDown");
        break;
      case EXPECTED_LATENCY:
        CrdUtils.validatePresent(expectedLatency, "expectedLatency");
        break;
      case THREAD_SATURATION:
        CrdUtils.validatePresent(threadSaturation, "threadSaturation");
        break;
      default:
        break;
    }
  }
}
