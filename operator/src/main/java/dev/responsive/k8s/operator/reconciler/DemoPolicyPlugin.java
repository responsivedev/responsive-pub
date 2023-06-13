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

import com.google.common.collect.ImmutableSet;
import dev.responsive.k8s.controller.ControllerProtoFactories;
import dev.responsive.k8s.crd.ResponsivePolicy;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.javaoperatorsdk.operator.api.config.informer.InformerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceInitializer;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.EventSource;
import io.javaoperatorsdk.operator.processing.event.source.informer.InformerEventSource;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import responsive.controller.v1.controller.proto.ControllerOuterClass;

public class DemoPolicyPlugin implements PolicyPlugin {
  private static final Logger LOGGER = LoggerFactory.getLogger(DemoPolicyPlugin.class);

  @Override
  public Map<String, EventSource> prepareEventSources(
      final EventSourceContext<ResponsivePolicy> ctx,
      final ResponsiveContext responsiveCtx
  ) {
    // TODO(rohan): switch this over to monitoring a statefulset instead
    final var deploymentEvents = new InformerEventSource<>(
        InformerConfiguration.from(Deployment.class, ctx)
            .withLabelSelector(ResponsivePolicyReconciler.NAME_LABEL)
            .withSecondaryToPrimaryMapper(DemoPolicyPlugin::toPrimaryMapper)
            .withPrimaryToSecondaryMapper(DemoPolicyPlugin::toDeploymentMapper)
            .build(),
        ctx
    );
    return EventSourceInitializer.nameEventSources(deploymentEvents);
  }


  @Override
  public void reconcile(
      final ResponsivePolicy policy,
      final Context<ResponsivePolicy> ctx,
      final ResponsiveContext responsiveCtx
  ) {
    final var appNamespace = policy.getSpec().getApplicationNamespace();
    final var appName = policy.getSpec().getApplicationName();
    final var deployment = getAndMaybeLabelCurrentDeployment(policy, ctx);

    LOGGER.info("Found deployment {} for app {}/{}", deployment, appNamespace, appName);

    responsiveCtx.getControllerClient().currentState(
        ControllerProtoFactories.currentStateRequest(policy, currentStateFromDeployment(deployment))
    );

    final var maybeTargetState = ctx.getSecondaryResource(TargetStateWithTimestamp.class);
    if (maybeTargetState.isPresent()) {
      final var targetState = maybeTargetState.get();
      LOGGER.info("Got update to target state {} {}", appName, targetState);
      final var targetReplicas = targetState.getTargetState().getDemoState().getReplicas();
      if (targetReplicas != deployment.getSpec().getReplicas()) {
        final var appClient = ctx.getClient().apps();
        // TODO(rohan): I don't think this is patching the way I expect. Review the patch APIs
        //              make sure its safe to assume the patch was applied if this succeeds
        appClient.deployments()
            .inNamespace(appNamespace)
            .withName(appName)
            .edit(d -> {
              if (d.getMetadata().getResourceVersion()
                  .equals(deployment.getMetadata().getResourceVersion())) {
                d.getSpec().setReplicas(targetReplicas);
              }
              return d;
            });
      }
    }
  }

  private Deployment getAndMaybeLabelCurrentDeployment(
      final ResponsivePolicy policy,
      final Context<ResponsivePolicy> ctx) {
    if (ctx.getSecondaryResource(Deployment.class).isPresent()) {
      final Deployment deployment = ctx.getSecondaryResource(Deployment.class).get();
      final Map<String, String> labels = deployment.getMetadata().getLabels();
      assert labels.containsKey(ResponsivePolicyReconciler.NAME_LABEL);
      assert
          labels.get(ResponsivePolicyReconciler.NAME_LABEL).equals(policy.getMetadata().getName());
      assert labels.containsKey(ResponsivePolicyReconciler.NAMESPACE_LABEL);
      assert labels.get(ResponsivePolicyReconciler.NAMESPACE_LABEL)
          .equals(policy.getMetadata().getNamespace());
      return deployment;
    } else {
      // The framework has no associated deployment yet, which means there is no deployment
      // with the required label. Label the deployment here.
      // TODO(rohan): double-check this understanding
      final Deployment deployment;
      final String appNs = policy.getSpec().getApplicationNamespace();
      final String app = policy.getSpec().getApplicationName();
      final var appClient = ctx.getClient().apps();
      deployment = appClient.deployments()
          .inNamespace(appNs)
          .withName(app)
          .get();
      if (deployment == null) {
        throw new RuntimeException(String.format("No deployment %s/%s found", appNs, app));
      }
      maybeAddResponsiveLabel(deployment, policy, ctx);
      return deployment;
    }
  }

  private void maybeAddResponsiveLabel(
      final Deployment deployment,
      final ResponsivePolicy policy,
      final Context<ResponsivePolicy> ctx) {
    final boolean hasNameLabel
        = deployment.getMetadata().getLabels().containsKey(ResponsivePolicyReconciler.NAME_LABEL);
    final boolean hasNamespaceLabel
        = deployment.getMetadata().getLabels()
        .containsKey(ResponsivePolicyReconciler.NAMESPACE_LABEL);
    if (!hasNameLabel || !hasNamespaceLabel) {
      final var appClient = ctx.getClient().apps();
      // TODO(rohan): I don't think this is patching the way I expect. Review the patch APIs
      //  things to check: do we need to check the version or does that happen automatically?
      //  things to check: this should only be updating the labels and thats it (e.g. truly a patch)
      //
      appClient.deployments()
          .inNamespace(deployment.getMetadata().getNamespace())
          .withName(deployment.getMetadata().getName())
          .edit(d -> {
            if (d.getMetadata().getResourceVersion()
                .equals(deployment.getMetadata().getResourceVersion())) {
              final HashMap<String, String> newLabels = new HashMap<>(d.getMetadata().getLabels());
              newLabels.put(
                  ResponsivePolicyReconciler.NAMESPACE_LABEL,
                  policy.getMetadata().getNamespace()
              );
              newLabels.put(ResponsivePolicyReconciler.NAME_LABEL, policy.getMetadata().getName());
              d.getMetadata().setLabels(newLabels);
            }
            return d;
          });
    }
  }

  private static ControllerOuterClass.ApplicationState currentStateFromDeployment(
      final Deployment deployment) {
    // TODO(rohan): need to include some indicator of whether or not deployment is stable
    //              (e.g. provisioned replicas are fully up or not)
    return ControllerOuterClass.ApplicationState.newBuilder()
        .setDemoState(
            ControllerOuterClass.DemoApplicationState.newBuilder()
                .setReplicas(deployment.getSpec().getReplicas()))
        .build();
  }

  private static Set<ResourceID> toPrimaryMapper(final HasMetadata hasMetadata) {
    if (!hasMetadata.getMetadata().getLabels().containsKey(ResponsivePolicyReconciler.NAME_LABEL)
        || !hasMetadata.getMetadata().getLabels()
        .containsKey(ResponsivePolicyReconciler.NAMESPACE_LABEL)) {
      return Collections.emptySet();
    }
    return ImmutableSet.of(
        new ResourceID(
            hasMetadata.getMetadata().getLabels().get(ResponsivePolicyReconciler.NAME_LABEL),
            hasMetadata.getMetadata().getLabels().get(ResponsivePolicyReconciler.NAMESPACE_LABEL)
        )
    );
  }

  private static Set<ResourceID> toDeploymentMapper(final ResponsivePolicy policy) {
    return ImmutableSet.of(
        new ResourceID(
            policy.getSpec().getApplicationName(),
            policy.getSpec().getApplicationNamespace())
    );
  }
}
