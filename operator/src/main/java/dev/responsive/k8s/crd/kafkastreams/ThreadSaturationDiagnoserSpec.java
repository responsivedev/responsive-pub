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

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Optional;

public class ThreadSaturationDiagnoserSpec {
  private final double threshold;
  private final Optional<Integer> windowSeconds;
  private final Optional<Integer> numWindows;
  private final Optional<Integer> graceSeconds;
  private final List<String> blockedMetric;


  public ThreadSaturationDiagnoserSpec(
      @JsonProperty("threshold") final double threshold,
      @JsonProperty("windowSeconds") final Optional<Integer> windowSeconds,
      @JsonProperty("numWindows") final Optional<Integer> numWindows,
      @JsonProperty("graceSeconds") final Optional<Integer> graceSeconds,
      @JsonProperty("blockedMetric") final List<String> blockedMetric
  ) {
    this.threshold = threshold;
    this.windowSeconds = windowSeconds;
    this.numWindows = numWindows;
    this.graceSeconds = graceSeconds;
    this.blockedMetric = blockedMetric;
  }

  public double getThreshold() {
    return threshold;
  }

  public Optional<Integer> getWindowSeconds() {
    return windowSeconds;
  }

  public Optional<Integer> getNumWindows() {
    return numWindows;
  }

  public Optional<Integer> getGraceSeconds() {
    return graceSeconds;
  }

  public List<String> getBlockedMetric() {
    return blockedMetric;
  }
}
