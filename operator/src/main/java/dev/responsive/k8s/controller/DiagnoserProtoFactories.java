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

package dev.responsive.k8s.controller;

import dev.responsive.k8s.crd.kafkastreams.DiagnoserSpec;
import dev.responsive.k8s.crd.kafkastreams.ExpectedLatencyDiagnoserSpec;
import dev.responsive.k8s.crd.kafkastreams.RateBasedDiagnoserSpec;
import dev.responsive.k8s.crd.kafkastreams.ThreadSaturationDiagnoserSpec;
import responsive.controller.v1.controller.proto.ControllerOuterClass;
import responsive.controller.v1.controller.proto.ControllerOuterClass.KafkaStreamsPolicySpec.FixedReplicaScaleUpStrategySpec;
import responsive.controller.v1.controller.proto.ControllerOuterClass.KafkaStreamsPolicySpec.RateBasedScaleUpStrategySpec;
import responsive.controller.v1.controller.proto.ControllerOuterClass.KafkaStreamsPolicySpec.ScaleToMaxStrategySpec;

public class DiagnoserProtoFactories {
  private static ControllerOuterClass.KafkaStreamsPolicySpec.RateBasedDiagnoserSpec
      rateBasedDiagnoserFromK8sResource(
          final RateBasedDiagnoserSpec diagnoser
  ) {
    final var builder = ControllerOuterClass.KafkaStreamsPolicySpec.RateBasedDiagnoserSpec
        .newBuilder()
        .setRate(diagnoser.getRate());
    if (diagnoser.getWindowMs().isPresent()) {
      builder.setWindowMs(diagnoser.getWindowMs().get());
    }
    return builder.build();
  }

  private static ControllerOuterClass.KafkaStreamsPolicySpec.ThreadSaturationDiagnoserSpec
      threadSaturationDiagnoserFromK8sResource(
          final ThreadSaturationDiagnoserSpec diagnoser
  ) {
    final var builder = ControllerOuterClass.KafkaStreamsPolicySpec.ThreadSaturationDiagnoserSpec
        .newBuilder()
        .setThreshold(diagnoser.getThreshold());
    diagnoser.getWindowSeconds().ifPresent(builder::setWindowSeconds);
    diagnoser.getNumWindows().ifPresent(builder::setNumWindows);
    diagnoser.getGraceSeconds().ifPresent(builder::setGraceSeconds);
    diagnoser.getBlockedMetric().forEach(builder::addBlockedMetric);
    return builder.build();
  }

  private static ControllerOuterClass.KafkaStreamsPolicySpec.ExpectedLatencyDiagnoserSpec
      expectedLatencyDiagnoserFromK8sResource(
          final ExpectedLatencyDiagnoserSpec diagnoser
  ) {
    final var builder = ControllerOuterClass.KafkaStreamsPolicySpec.ExpectedLatencyDiagnoserSpec
        .newBuilder()
        .setMaxExpectedLatencySeconds(diagnoser.getMaxExpectedLatencySeconds());
    diagnoser.getWindowSeconds().ifPresent(builder::setWindowSeconds);
    diagnoser.getProjectionSeconds().ifPresent(builder::setProjectionSeconds);
    diagnoser.getGraceSeconds().ifPresent(builder::setGraceSeconds);
    diagnoser.getScaleDownBufferSeconds().ifPresent(builder::setScaleDownBufferSeconds);
    diagnoser.getStaggerSeconds().ifPresent(builder::setStaggerSeconds);
    final var type = diagnoser.getScaleUpStrategy().getType();
    switch (type) {
      case RATE_BASED: {
        builder.setRateBased(RateBasedScaleUpStrategySpec.newBuilder().build());
        break;
      }
      case SCALE_TO_MAX: {
        builder.setScaleToMax(ScaleToMaxStrategySpec.newBuilder().build());
        break;
      }
      case FIXED_REPLICA: {
        builder.setFixedReplicas(FixedReplicaScaleUpStrategySpec.newBuilder()
            .setReplicas(diagnoser.getScaleUpStrategy().getFixedReplica().get().getReplicas())
            .build()
        );
        break;
      }
      default:
        throw new IllegalStateException("unexpected strategy type: " + type);
    }
    return builder.build();
  }

  static ControllerOuterClass.KafkaStreamsPolicySpec.DiagnoserSpec
      diagnoserFromK8sResource(final DiagnoserSpec diagnoserSpec) {
    switch (diagnoserSpec.getType()) {
      case LAG_SCALE_UP:
        return ControllerOuterClass.KafkaStreamsPolicySpec.DiagnoserSpec.newBuilder()
            .setLagScaleUp(ControllerOuterClass.KafkaStreamsPolicySpec.LagDiagnoserSpec
                .newBuilder().build())
            .build();
      case PROCESSING_RATE_SCALE_UP: {
        return ControllerOuterClass.KafkaStreamsPolicySpec.DiagnoserSpec.newBuilder()
            .setProcessingRateScaleUp(
                rateBasedDiagnoserFromK8sResource(
                    diagnoserSpec.getProcessingRateScaleUp().get()))
            .build();
      }
      case PROCESSING_RATE_SCALE_DOWN: {
        return ControllerOuterClass.KafkaStreamsPolicySpec.DiagnoserSpec.newBuilder()
            .setProcessingRateScaleDown(
                rateBasedDiagnoserFromK8sResource(
                    diagnoserSpec.getProcessingRateScaleDown().get()))
            .build();
      }
      case ARRIVAL_RATE_SCALE_UP: {
        return ControllerOuterClass.KafkaStreamsPolicySpec.DiagnoserSpec.newBuilder()
            .setArrivalRateScaleUp(
                rateBasedDiagnoserFromK8sResource(
                    diagnoserSpec.getArrivalRateScaleUp().get()))
            .build();

      }
      case EXPECTED_LATENCY: {
        return ControllerOuterClass.KafkaStreamsPolicySpec.DiagnoserSpec.newBuilder()
            .setExpectedLatency(
                expectedLatencyDiagnoserFromK8sResource(
                    diagnoserSpec.getExpectedLatency().get()))
            .build();
      }
      case THREAD_SATURATION: {
        return ControllerOuterClass.KafkaStreamsPolicySpec.DiagnoserSpec.newBuilder()
            .setThreadSaturation(
                threadSaturationDiagnoserFromK8sResource(
                    diagnoserSpec.getThreadSaturation().get()))
            .build();
      }
      default:
        throw new IllegalStateException();
    }
  }
}
