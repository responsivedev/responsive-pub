/*
 * Copyright 2023 Responsive Computing, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dev.responsive.k8s.operator;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import dev.responsive.controller.client.ControllerClient;
import dev.responsive.k8s.crd.ResponsivePolicy;
import dev.responsive.k8s.crd.ResponsivePolicySpec;
import dev.responsive.k8s.crd.kafkastreams.KafkaStreamsPolicySpec;
import dev.responsive.k8s.operator.reconciler.ResponsivePolicyReconciler;
import io.fabric8.kubernetes.api.model.Namespace;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinition;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.utils.Serialization;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.javaoperatorsdk.operator.Operator;
import io.javaoperatorsdk.operator.api.config.ControllerConfiguration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.junit.jupiter.TestcontainersExtension;
import org.testcontainers.k3s.K3sContainer;
import org.testcontainers.utility.DockerImageName;
import responsive.controller.v1.controller.proto.ControllerOuterClass;

@Testcontainers
@ExtendWith({TestcontainersExtension.class})
public class OperatorE2EIntegrationTest {

  @Container
  private static final K3sContainer K3S =
      new K3sContainer(DockerImageName.parse("rancher/k3s:v1.21.3-k3s1"));

  private static KubernetesClient kubernetesClient;

  @BeforeAll
  public static void setUp() {
    Serialization.jsonMapper().registerModule(new Jdk8Module());

    K3S.start();
    Config config = Config.fromKubeconfig(K3S.getKubeConfigYaml());
    kubernetesClient = new KubernetesClientBuilder()
        .withConfig(config)
        .build();

    Namespace namespace = new NamespaceBuilder()
        .withNewMetadata()
        .withName("test")
        .endMetadata()
        .build();

    kubernetesClient.namespaces().resource(namespace).create();

    // Path to the generated YAML file
    String crdPath = "META-INF/fabric8/responsivepolicies.application.responsive.dev-v1.yml";

    // Read the YAML file and convert it to a CRD object
    CustomResourceDefinition crd =
        Serialization.unmarshal(
            OperatorE2EIntegrationTest.class.getClassLoader().getResourceAsStream(crdPath),
            CustomResourceDefinition.class);

    // Register the CRD
    kubernetesClient.apiextensions().v1().customResourceDefinitions().resource(crd).create();
  }

  @AfterAll
  public static void tearDown() {
    kubernetesClient.namespaces().withName("test").delete();
    K3S.stop();
  }

  @Test
  @Timeout(30_000L)
  public void testReconcileLabelSelector() throws InterruptedException {
    // Given:
    final var latch = new CountDownLatch(1);
    final var controllerClient = mock(ControllerClient.class);
    doAnswer(iom -> {
      latch.countDown();
      return null;
    }).when(controllerClient).upsertPolicy(any());
    when(controllerClient.getTargetState(any()))
        .thenThrow(new StatusRuntimeException(Status.NOT_FOUND));

    final var reconciler = new ResponsivePolicyReconciler("", controllerClient);
    final var operator = new Operator(kubernetesClient);
    operator.register(
        reconciler,
        new ControllerConfiguration<>() {
          @Override
          public String getAssociatedReconcilerClassName() {
            return ResponsivePolicyReconciler.class.getName();
          }

          @Override
          public String getLabelSelector() {
            return "environment=test";
          }
        }
    );

    operator.start();

    // When:

    // this one doesn't get registered at all
    kubernetesClient.resources(ResponsivePolicy.class)
        .resource(createPolicy("one", Map.of()))
        .create();

    // this one counts down the latch
    kubernetesClient.resources(ResponsivePolicy.class)
        .resource(createPolicy("two", Map.of("environment", "test")))
        .create();

    latch.await();

    // Then:
    Mockito.verify(controllerClient, Mockito.times(1)).upsertPolicy(any());
  }

  private static ResponsivePolicy createPolicy(
      final String name,
      final Map<String, String> labels
  ) {
    final ResponsivePolicy policy = new ResponsivePolicy();
    policy.setMetadata(new ObjectMeta());
    policy.getMetadata().setNamespace("test");
    policy.getMetadata().setName(name);
    policy.getMetadata().setLabels(labels);
    policy.setSpec(
        new ResponsivePolicySpec(
            "biz",
            "baz",
            "bop",
            ControllerOuterClass.PolicyStatus.POLICY_STATUS_MANAGED,
            ResponsivePolicySpec.PolicyType.KAFKA_STREAMS,
            Optional.empty(),
            Optional.of(new KafkaStreamsPolicySpec(
                3,
                1,
                1,
                Optional.empty(),
                Optional.empty()
            ))
        )
    );
    return policy;
  }

}
