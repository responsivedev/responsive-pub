package dev.responsive.k8s.controller;

import dev.responsive.k8s.crd.kafkastreams.DemoPolicySpec;
import dev.responsive.k8s.crd.kafkastreams.DiagnoserSpec;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import responsive.controller.v1.controller.proto.ControllerOuterClass;

final class DemoPolicyProtoFactories {

  private DemoPolicyProtoFactories() {
  }

  static List<ControllerOuterClass.DemoPolicySpec.DiagnoserSpec> diagnosersFromK8sResource(
      final Optional<List<DiagnoserSpec>> diagnosers
  ) {
    if (diagnosers.isEmpty()) {
      return Collections.emptyList();
    }
    return diagnosers.get().stream()
        .map(DiagnoserProtoFactories::diagnoserFromK8sResource)
        .collect(Collectors.toList());
  }

  static ControllerOuterClass.DemoPolicySpec demoPolicyFromK8sResource(
      final DemoPolicySpec demoPolicy) {
    return ControllerOuterClass.DemoPolicySpec.newBuilder()
        .setMaxReplicas(demoPolicy.getMaxReplicas())
        .setMinReplicas(demoPolicy.getMinReplicas())
        .addAllDiagnoser(
            DemoPolicyProtoFactories.diagnosersFromK8sResource(demoPolicy.getDiagnosers()))
        .build();
  }
}
