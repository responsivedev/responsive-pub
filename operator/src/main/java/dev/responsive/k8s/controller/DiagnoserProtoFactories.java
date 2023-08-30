package dev.responsive.k8s.controller;

import dev.responsive.k8s.crd.kafkastreams.DiagnoserSpec;
import dev.responsive.k8s.crd.kafkastreams.RateBasedDiagnoserSpec;
import responsive.controller.v1.controller.proto.ControllerOuterClass;

public class DiagnoserProtoFactories {
  private static ControllerOuterClass.DemoPolicySpec.RateBasedDiagnoserSpec
      rateBasedDiagnoserFromK8sResource(
          final RateBasedDiagnoserSpec diagnoser
  ) {
    final var builder = ControllerOuterClass.DemoPolicySpec.RateBasedDiagnoserSpec.newBuilder()
        .setRate(diagnoser.getRate());
    if (diagnoser.getWindowMs().isPresent()) {
      builder.setWindowMs(diagnoser.getWindowMs().get());
    }
    return builder.build();
  }

  static ControllerOuterClass.DemoPolicySpec.DiagnoserSpec
      diagnoserFromK8sResource(final DiagnoserSpec diagnoserSpec) {
    switch (diagnoserSpec.getType()) {
      case LAG_SCALE_UP:
        return ControllerOuterClass.DemoPolicySpec.DiagnoserSpec.newBuilder()
            .setLagScaleUp(ControllerOuterClass.DemoPolicySpec.LagDiagnoserSpec
                .newBuilder().build())
            .build();
      case PROCESSING_RATE_SCALE_UP: {
        return ControllerOuterClass.DemoPolicySpec.DiagnoserSpec.newBuilder()
            .setProcessingRateScaleUp(
                rateBasedDiagnoserFromK8sResource(
                    diagnoserSpec.getProcessingRateScaleUp().get()))
            .build();
      }
      case PROCESSING_RATE_SCALE_DOWN: {
        return ControllerOuterClass.DemoPolicySpec.DiagnoserSpec.newBuilder()
            .setProcessingRateScaleDown(
                rateBasedDiagnoserFromK8sResource(
                    diagnoserSpec.getProcessingRateScaleDown().get()))
            .build();
      }
      default:
        throw new IllegalStateException();
    }
  }
}
