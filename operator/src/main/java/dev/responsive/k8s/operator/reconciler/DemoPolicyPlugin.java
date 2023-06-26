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
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.javaoperatorsdk.operator.api.config.informer.InformerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceInitializer;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.EventSource;
import io.javaoperatorsdk.operator.processing.event.source.informer.InformerEventSource;
import java.util.Collections;
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
            .withPrimaryToSecondaryMapper(DemoPolicyPlugin::toApplicationMapper)
            .build(),
        ctx
    );

    final var statefulSetEvents = new InformerEventSource<>(
        InformerConfiguration.from(StatefulSet.class, ctx)
            .withLabelSelector(ResponsivePolicyReconciler.NAME_LABEL)
            .withSecondaryToPrimaryMapper(DemoPolicyPlugin::toPrimaryMapper)
            .withPrimaryToSecondaryMapper(DemoPolicyPlugin::toApplicationMapper)
            .build(),
        ctx
    );

    return EventSourceInitializer.nameEventSources(deploymentEvents, statefulSetEvents);
  }


  @Override
  public void reconcile(
      final ResponsivePolicy policy,
      final Context<ResponsivePolicy> ctx,
      final ResponsiveContext responsiveCtx
  ) {
    final var appNamespace = policy.getSpec().getApplicationNamespace();
    final var appName = policy.getSpec().getApplicationName();
    final var managedApp = ManagedApplication.build(ctx, policy);

    LOGGER.info("Found type {} for app {}/{}", managedApp.appType(), appNamespace, appName);

    responsiveCtx.getControllerClient().currentState(
        ControllerProtoFactories.currentStateRequest(policy,
            currentStateFromApplication(managedApp))
    );

    final var maybeTargetState =
        ctx.getSecondaryResource(TargetStateWithTimestamp.class);
    if (maybeTargetState.isEmpty()) {
      LOGGER.warn("No target state present in ctx. This should not happen");
      return;
    }

    final var targetState = maybeTargetState.get();
    LOGGER.info("target state for app {} {}", appName, targetState);

    if (targetState.getTargetState().isEmpty()) {
      LOGGER.info(
          "we were not able to get a target state from controller, so don't try to reconcile one");
      return;
    }
    final var targetReplicas = targetState.getTargetState().get().getDemoState().getReplicas();
    if (targetReplicas != managedApp.getReplicas()) {
      LOGGER.info(
          "Scaling {}/{} from {} to {}",
          appNamespace,
          appName,
          managedApp.getReplicas(),
          targetReplicas
      );

      // TODO(rohan): I don't think this is patching the way I expect. Review the patch APIs
      //              make sure its safe to assume the patch was applied if this succeeds
      managedApp.setReplicas(targetReplicas, ctx);
    }
  }

  private static ControllerOuterClass.ApplicationState currentStateFromApplication(
      final ManagedApplication application) {
    // TODO(rohan): need to include some indicator of whether or not deployment is stable
    //              (e.g. provisioned replicas are fully up or not)
    return ControllerOuterClass.ApplicationState.newBuilder()
        .setDemoState(
            ControllerOuterClass.DemoApplicationState.newBuilder()
                .setReplicas(application.getReplicas()))
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

  private static Set<ResourceID> toApplicationMapper(final ResponsivePolicy policy) {
    return ImmutableSet.of(
        new ResourceID(
            policy.getSpec().getApplicationName(),
            policy.getSpec().getApplicationNamespace())
    );
  }
}
