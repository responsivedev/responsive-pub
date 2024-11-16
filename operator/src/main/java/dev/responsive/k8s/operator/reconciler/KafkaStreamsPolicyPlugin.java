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

package dev.responsive.k8s.operator.reconciler;

import com.google.common.collect.ImmutableSet;
import dev.responsive.controller.client.ControllerClient;
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
import responsive.controller.v1.controller.proto.ControllerOuterClass.Action;
import responsive.controller.v1.controller.proto.ControllerOuterClass.UpdateActionStatusRequest;

public class KafkaStreamsPolicyPlugin implements PolicyPlugin {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaStreamsPolicyPlugin.class);

  private final String environment;

  public KafkaStreamsPolicyPlugin(final String environment) {
    this.environment = environment;
  }

  @Override
  public Map<String, EventSource> prepareEventSources(
      final EventSourceContext<ResponsivePolicy> ctx,
      final ResponsiveContext responsiveCtx
  ) {
    final var deploymentEvents = new InformerEventSource<>(
        InformerConfiguration.from(Deployment.class, ctx)
            .withLabelSelector(ResponsivePolicyReconciler.NAME_LABEL)
            .withSecondaryToPrimaryMapper(KafkaStreamsPolicyPlugin::toPrimaryMapper)
            .withPrimaryToSecondaryMapper(KafkaStreamsPolicyPlugin::toApplicationMapper)
            .build(),
        ctx
    );

    final var statefulSetEvents = new InformerEventSource<>(
        InformerConfiguration.from(StatefulSet.class, ctx)
            .withLabelSelector(ResponsivePolicyReconciler.NAME_LABEL)
            .withSecondaryToPrimaryMapper(KafkaStreamsPolicyPlugin::toPrimaryMapper)
            .withPrimaryToSecondaryMapper(KafkaStreamsPolicyPlugin::toApplicationMapper)
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

    LOG.info("Found type {} for app {}/{}", managedApp.appType(), appNamespace, appName);

    final var controllerClient = responsiveCtx.getControllerClient();
    controllerClient.currentState(
        ControllerProtoFactories.currentStateRequest(
            environment,
            policy,
            currentStateFromApplication(managedApp))
    );

    final var maybeTargetState =
        ctx.getSecondaryResource(ActionsWithTimestamp.class);
    if (maybeTargetState.isEmpty()) {
      LOG.warn("No target state present in ctx. This should not happen");
      return;
    }

    final var targetState = maybeTargetState.get();
    LOG.info("target state for app {} {}", appName, targetState);

    if (targetState.getTargetState().isPresent()) {
      LOG.debug(
          "we were not able to get a target state from controller, either due to error or"
              + "there not being a current target. so don't try to reconcile one");
      maybeScaleApplication(
          targetState.getTargetState().get().getKafkaStreamsState().getReplicas(),
          managedApp,
          appNamespace,
          appName,
          ctx
      );
    }

    for (final Action action : targetState.getActions()) {
      switch (action.getActionCase()) {
        case SCALE_APPLICATION:
          maybeScaleApplication(
              action.getScaleApplication().getReplicas(),
              managedApp,
              appNamespace,
              appName,
              ctx
          );
          controllerClient.updateActionStatus(actionSuccess(environment, policy, action));
          break;
        case RESTART_POD:
          restartPod(
              environment, policy, action, managedApp, controllerClient, ctx);
          break;
        default:
          controllerClient.updateActionStatus(
              actionFailed(
                  environment,
                  policy,
                  action,
                  "Action is not suppported by operator")
          );
          break;
      }
    }
  }

  private void restartPod(
      final String environment,
      final ResponsivePolicy policy,
      final ControllerOuterClass.Action restartPod,
      final ManagedApplication managedApp,
      final ControllerClient controllerClient,
      final Context<ResponsivePolicy> context
  ) {
    LOG.info("executing restart pod for {}", restartPod.getRestartPod().getPodId());
    try {
      managedApp.restartPod(restartPod.getRestartPod().getPodId(), context);
    } catch (final RuntimeException e) {
      // just pass exceptions back up to the controller for this action
      LOG.error("pod restart failed with", e);
      controllerClient.updateActionStatus(
          actionFailed(
              environment,
              policy,
              restartPod,
              "restart failed with: " + e.getMessage()
          )
      );
      return;
    }
    controllerClient.updateActionStatus(actionSuccess(environment, policy, restartPod));
  }

  private UpdateActionStatusRequest actionSuccess(
      final String env,
      final ResponsivePolicy policy,
      final Action action
  ) {
    return ControllerProtoFactories.updateActionStatusRequest(
        env,
        policy,
        action.getId(),
        ControllerOuterClass.ActionStatus.Status.COMPLETED,
        "Action completed successfully"
    );
  }

  private UpdateActionStatusRequest actionFailed(
      final String env,
      final ResponsivePolicy policy,
      final Action action,
      final String reason
  ) {
    return ControllerProtoFactories.updateActionStatusRequest(
        env,
        policy,
        action.getId(),
        ControllerOuterClass.ActionStatus.Status.FAILED,
        reason
    );
  }

  private void maybeScaleApplication(
      final int targetReplicas,
      final ManagedApplication managedApp,
      final String appNamespace,
      final String appName,
      final Context<ResponsivePolicy> ctx
  ) {
    if (targetReplicas != managedApp.getReplicas()) {
      LOG.info(
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
        .setKafkaStreamsState(
            ControllerOuterClass.KafkaStreamsApplicationState.newBuilder()
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
