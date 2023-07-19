package dev.responsive.k8s.controller;

import dev.responsive.k8s.crd.DemoPolicy;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import responsive.controller.v1.controller.proto.ControllerOuterClass;
import responsive.controller.v1.controller.proto.ControllerOuterClass.DemoPolicy.Diagnoser;
import responsive.controller.v1.controller.proto.ControllerOuterClass.DemoPolicy.LagDiagnoser;
import responsive.controller.v1.controller.proto.ControllerOuterClass.DemoPolicy.ProcessingRateDiagnoser;

final class DemoPolicyProtoFactories {

  private DemoPolicyProtoFactories() {
  }

  private static ProcessingRateDiagnoser processingRateDiagnoserFromK8sResource(
      final DemoPolicy.ProcessingRateDiagnoser diagnoser
  ) {
    final var builder = ProcessingRateDiagnoser.newBuilder()
        .setRate(diagnoser.getRate());
    if (diagnoser.getWindowMs().isPresent()) {
      builder.setWindowMs(diagnoser.getWindowMs().get());
    }
    return builder.build();
  }

  static Diagnoser diagnoserFromK8sResource(final DemoPolicy.Diagnoser diagnoser) {
    switch (diagnoser.getType()) {
      case LAG_SCALE_UP:
        return Diagnoser.newBuilder()
            .setLagScaleUp(LagDiagnoser.newBuilder().build())
            .build();
      case PROCESSING_RATE_SCALE_UP: {
        return Diagnoser.newBuilder()
            .setProcessingRateScaleUp(
                processingRateDiagnoserFromK8sResource(
                    diagnoser.getProcessingRateScaleUp().get()))
            .build();
      }
      case PROCESSING_RATE_SCALE_DOWN: {
        return Diagnoser.newBuilder()
            .setProcessingRateScaleDown(
                processingRateDiagnoserFromK8sResource(
                    diagnoser.getProcessingRateScaleDown().get()))
            .build();
      }
      default:
        throw new IllegalStateException();
    }
  }

  private static List<Diagnoser> diagnosersFromK8sResource(
      final Optional<List<DemoPolicy.Diagnoser>> diagnosers
  ) {
    if (diagnosers.isEmpty()) {
      return Collections.emptyList();
    }
    return diagnosers.get().stream()
        .map(DemoPolicyProtoFactories::diagnoserFromK8sResource)
        .collect(Collectors.toList());
  }

  static ControllerOuterClass.DemoPolicy demoPolicyFromK8sResource(final DemoPolicy demoPolicy) {
    return ControllerOuterClass.DemoPolicy.newBuilder()
        .setMaxReplicas(demoPolicy.getMaxReplicas())
        .setMinReplicas(demoPolicy.getMinReplicas())
        .addAllDiagnoser(DemoPolicyProtoFactories.diagnosersFromK8sResource(demoPolicy.getDiagnosers()))
        .build();
  }
}
