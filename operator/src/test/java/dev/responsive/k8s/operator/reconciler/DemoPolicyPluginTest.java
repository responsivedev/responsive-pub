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

package dev.responsive.k8s.operator.reconciler;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import dev.responsive.controller.client.ControllerClient;
import dev.responsive.k8s.controller.ControllerProtoFactories;
import dev.responsive.k8s.crd.ResponsivePolicy;
import dev.responsive.k8s.crd.ResponsivePolicySpec;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentList;
import io.fabric8.kubernetes.api.model.apps.DeploymentSpec;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.AppsAPIGroupDSL;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.RollableScalableResource;
import io.javaoperatorsdk.operator.api.config.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.EventSource;
import io.javaoperatorsdk.operator.processing.event.source.informer.InformerEventSource;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.UnaryOperator;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import responsive.controller.v1.controller.proto.ControllerOuterClass;
import responsive.controller.v1.controller.proto.ControllerOuterClass.PolicyStatus;

@ExtendWith(MockitoExtension.class)
class DemoPolicyPluginTest {
  @Mock
  private EventSourceContext<ResponsivePolicy> esCtx;
  @Mock
  private Context<ResponsivePolicy> ctx;
  @Mock
  private ControllerConfiguration<ResponsivePolicy> controllerConfig;
  @Mock
  private KubernetesClient client;
  @Mock
  private AppsAPIGroupDSL appsClient;
  @Mock
  private MixedOperation<Deployment, DeploymentList, RollableScalableResource<Deployment>>
      deploymentsClient;
  @Mock
  private NonNamespaceOperation<Deployment, DeploymentList, RollableScalableResource<Deployment>>
      nsDeploymentsClient;
  @Mock
  private RollableScalableResource<Deployment> rsDeployment;
  @Captor
  private ArgumentCaptor<UnaryOperator<Deployment>> deploymentEdit;
  @Captor
  private ArgumentCaptor<ControllerOuterClass.CurrentStateRequest> currentStateRequestCaptor;
  @Mock
  private ControllerClient controllerClient;

  private final dev.responsive.k8s.operator.reconciler.DemoPolicyPlugin
      plugin = new dev.responsive.k8s.operator.reconciler.DemoPolicyPlugin();
  private final Deployment deployment = new Deployment();
  private final ResponsivePolicy policy = new ResponsivePolicy();
  private final ControllerOuterClass.ApplicationState targetState =
      ControllerOuterClass.ApplicationState.newBuilder()
          .setDemoState(ControllerOuterClass.DemoApplicationState.newBuilder()
              .setReplicas(5)
              .build())
          .build();
  private dev.responsive.k8s.operator.reconciler.ResponsiveContext responsiveCtx;

  @BeforeEach
  public void setup() {
    initDeployment(
        deployment,
        "baz",
        "biz",
        "v1",
        3,
        ImmutableMap.of(
            dev.responsive.k8s.operator.reconciler.ResponsivePolicyReconciler.NAME_LABEL, "bar",
            dev.responsive.k8s.operator.reconciler.ResponsivePolicyReconciler.NAMESPACE_LABEL, "foo"
        )
    );

    policy.setMetadata(new ObjectMeta());
    policy.getMetadata().setNamespace("foo");
    policy.getMetadata().setName("bar");
    policy.setSpec(
        new ResponsivePolicySpec(
            "biz",
            "baz",
            PolicyStatus.POLICY_STATUS_MANAGED,
            ResponsivePolicySpec.PolicyType.DEMO,
            Optional.of(new ResponsivePolicySpec.DemoPolicy(123))
        )
    );

    responsiveCtx = new dev.responsive.k8s.operator.reconciler.ResponsiveContext(controllerClient);

    lenient().when(esCtx.getControllerConfiguration()).thenReturn(controllerConfig);
    lenient().when(controllerConfig.getEffectiveNamespaces())
        .thenReturn(ImmutableSet.of("responsive"));
    lenient().when(esCtx.getClient()).thenReturn(client);

    lenient().when(ctx.getClient()).thenReturn(client);
    lenient().when(client.apps()).thenReturn(appsClient);
    lenient().when(appsClient.deployments()).thenReturn(deploymentsClient);
    setupDeploymentToBeReturned(deployment);
    lenient().when(ctx.getSecondaryResource(Deployment.class)).thenReturn(Optional.of(deployment));
    lenient().when(ctx.getSecondaryResource(
            dev.responsive.k8s.operator.reconciler.TargetStateWithTimestamp.class))
        .thenReturn(Optional.of(new TargetStateWithTimestamp(targetState)));
  }

  @Test
  public void shouldAddDeploymentEventSource() {
    // when:
    final var sources = plugin.prepareEventSources(esCtx, responsiveCtx);

    // then:
    final Optional<InformerEventSource<Deployment, ResponsivePolicy>> src
        = maybePullSrc(sources, Deployment.class);
    assertThat(src.isPresent(), is(true));
  }

  @Test
  public void shouldSetSecondaryMapperForDeploymentEventSource() {
    // when:
    final var sources = plugin.prepareEventSources(esCtx, responsiveCtx);

    // then:
    final Optional<InformerEventSource<Deployment, ResponsivePolicy>> src
        = maybePullSrc(sources, Deployment.class);
    assert src.isPresent();
    final var s2pMapper = src.get().getConfiguration().getSecondaryToPrimaryMapper();
    final var ids = s2pMapper.toPrimaryResourceIDs(deployment);
    assertThat(ids, contains(new ResourceID("bar", "foo")));
  }

  @Test
  public void shouldSetPrimaryToSecondaryMapperForDeploymentEventSource() {
    // when:
    final var sources = plugin.prepareEventSources(esCtx, responsiveCtx);

    // then:
    final Optional<InformerEventSource<Deployment, ResponsivePolicy>> src
        = maybePullSrc(sources, Deployment.class);
    assert src.isPresent();
    final var s2pMapper = src.get().getConfiguration().getPrimaryToSecondaryMapper();
    final var ids = s2pMapper.toSecondaryResourceIDs(policy);
    assertThat(ids, contains(new ResourceID("baz", "biz")));
  }

  @Test
  public void shouldPatchDeploymentWithReferenceToPolicy() {
    // given:
    when(ctx.getSecondaryResource(Deployment.class)).thenReturn(Optional.empty());
    final var deployment = createDeployment("baz", "biz", "v1", 5, Collections.emptyMap());
    setupDeploymentToBeReturned(deployment);

    // when:
    plugin.reconcile(policy, ctx, responsiveCtx);

    // then:
    verify(rsDeployment).edit(deploymentEdit.capture());
    final var edit = deploymentEdit.getValue();
    final var blank = createDeployment(
        "baz", "biz", "v1", 3, Collections.emptyMap()
    );
    edit.apply(blank);
    assertThat(blank.getMetadata().getLabels().get(
            dev.responsive.k8s.operator.reconciler.ResponsivePolicyReconciler.NAMESPACE_LABEL),
        is("foo"));
    assertThat(blank.getMetadata().getLabels().get(
            dev.responsive.k8s.operator.reconciler.ResponsivePolicyReconciler.NAME_LABEL),
        is("bar"));
  }

  @Test
  public void shouldReportCurrentState() {
    // when:
    plugin.reconcile(policy, ctx, responsiveCtx);

    // then:
    verify(controllerClient).currentState(currentStateRequestCaptor.capture());
    final var currentStateRequest = currentStateRequestCaptor.getValue();
    MatcherAssert.assertThat(
        currentStateRequest,
        equalTo(ControllerProtoFactories.currentStateRequest(
                policy,
                ControllerOuterClass.ApplicationState.newBuilder()
                    .setDemoState(
                        ControllerOuterClass.DemoApplicationState.newBuilder()
                            .setReplicas(3)
                            .build())
                    .build()
            )
        )
    );
  }

  @Test
  public void shouldPatchDeploymentIfReplicasChanged() {
    // when:
    plugin.reconcile(policy, ctx, responsiveCtx);

    // then:
    verify(rsDeployment).edit(deploymentEdit.capture());
    final var edit = deploymentEdit.getValue();
    final var blank = createDeployment("biz", "baz", "v1", 3, Collections.emptyMap());
    edit.apply(blank);
    assertThat(blank.getSpec().getReplicas(), is(5));
  }

  @Test
  public void shouldNotPatchDeploymentIfReplicasNotChanged() {
    // given:
    deployment.getSpec().setReplicas(5);

    // when:
    plugin.reconcile(policy, ctx, responsiveCtx);

    // then:
    verifyNoInteractions(rsDeployment);
  }

  @Test
  public void shouldNotPatchDeploymentIfNoTargetStateSpecified() {
    // given:
    when(ctx.getSecondaryResource(TargetStateWithTimestamp.class))
        .thenReturn(Optional.of(new TargetStateWithTimestamp()));

    // when:
    plugin.reconcile(policy, ctx, responsiveCtx);

    // then:
    verifyNoInteractions(rsDeployment);
  }

  @SuppressWarnings("unchecked")
  private <R extends HasMetadata> Optional<InformerEventSource<R, ResponsivePolicy>> maybePullSrc(
      final Map<String, EventSource> sources,
      final Class<R> clazz
  ) {
    for (final EventSource source : sources.values()) {
      if (source instanceof InformerEventSource<?, ?>) {
        if (((InformerEventSource<?, ?>) source).getConfiguration().getResourceClass()
            .equals(clazz)) {
          return Optional.of((InformerEventSource<R, ResponsivePolicy>) source);
        }
      }
    }
    return Optional.empty();
  }

  private void setupDeploymentToBeReturned(final Deployment deployment) {
    lenient().when(deploymentsClient.inNamespace(deployment.getMetadata().getNamespace()))
        .thenReturn(nsDeploymentsClient);
    lenient().when(nsDeploymentsClient.withName(deployment.getMetadata().getName()))
        .thenReturn(rsDeployment);
    lenient().when(rsDeployment.get()).thenReturn(deployment);

    final DeploymentList list = new DeploymentList();
    list.setItems(List.of(deployment));
    lenient().when(nsDeploymentsClient.list()).thenReturn(list);
  }

  private Deployment createDeployment(
      final String name,
      final String namespace,
      final String version,
      int replicas,
      final Map<String, String> labels
  ) {
    final Deployment deployment = new Deployment();
    initDeployment(deployment, name, namespace, version, replicas, labels);
    return deployment;
  }

  private void initDeployment(
      final Deployment deployment,
      final String name,
      final String namespace,
      final String version,
      int replicas,
      final Map<String, String> labels
  ) {
    deployment.setMetadata(new ObjectMeta());
    deployment.getMetadata().setNamespace(namespace);
    deployment.getMetadata().setName(name);
    deployment.getMetadata().setLabels(labels);
    deployment.getMetadata().setResourceVersion(version);
    deployment.setSpec(new DeploymentSpec());
    deployment.getSpec().setReplicas(replicas);
  }
}