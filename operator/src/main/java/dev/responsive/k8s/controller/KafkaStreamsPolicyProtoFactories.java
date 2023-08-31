package dev.responsive.k8s.controller;

import dev.responsive.k8s.crd.kafkastreams.DiagnoserSpec;
import dev.responsive.k8s.crd.kafkastreams.KafkaStreamsPolicySpec;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import responsive.controller.v1.controller.proto.ControllerOuterClass;

final class KafkaStreamsPolicyProtoFactories {

  private KafkaStreamsPolicyProtoFactories() {
  }

  static List<ControllerOuterClass.KafkaStreamsPolicySpec.DiagnoserSpec> diagnosersFromK8sResource(
      final Optional<List<DiagnoserSpec>> diagnosers
  ) {
    if (diagnosers.isEmpty()) {
      return Collections.emptyList();
    }
    return diagnosers.get().stream()
        .map(DiagnoserProtoFactories::diagnoserFromK8sResource)
        .collect(Collectors.toList());
  }

  static ControllerOuterClass.KafkaStreamsPolicySpec kafkaStreamsPolicyFromK8sResource(
      final KafkaStreamsPolicySpec kafkaStreamsPolicy) {
    return ControllerOuterClass.KafkaStreamsPolicySpec.newBuilder()
        .setMaxReplicas(kafkaStreamsPolicy.getMaxReplicas())
        .setMinReplicas(kafkaStreamsPolicy.getMinReplicas())
        .setMaxScaleUpReplicas(kafkaStreamsPolicy.getMaxScaleUpReplicas())
        .addAllDiagnoser(
            KafkaStreamsPolicyProtoFactories.diagnosersFromK8sResource(
                kafkaStreamsPolicy.getDiagnosers()))
        .build();
  }
}
