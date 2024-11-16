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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import dev.responsive.controller.client.ControllerClient;
import dev.responsive.k8s.controller.ControllerProtoFactories;
import dev.responsive.k8s.crd.ResponsivePolicy;
import dev.responsive.k8s.crd.ResponsivePolicySpec;
import dev.responsive.k8s.crd.ResponsivePolicySpec.PolicyType;
import dev.responsive.k8s.crd.ResponsivePolicyStatus;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceInitializer;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import io.javaoperatorsdk.operator.processing.event.source.EventSource;
import io.javaoperatorsdk.operator.processing.event.source.polling.PerResourcePollingEventSource;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import responsive.controller.v1.controller.proto.ControllerOuterClass;
import responsive.controller.v1.controller.proto.ControllerOuterClass.ApplicationState;

/**
 * Core reconciliation handler for operator
 */
@ControllerConfiguration
public class ResponsivePolicyReconciler implements
    Reconciler<ResponsivePolicy>, EventSourceInitializer<ResponsivePolicy> {
  private static final Logger LOG = LoggerFactory.getLogger(ResponsivePolicyReconciler.class);

  public static final String NAME_LABEL = "responsivePolicyName";
  public static final String NAMESPACE_LABEL = "responsivePolicyNamespace";

  private final ResponsiveContext responsiveCtx;
  private final String environment;
  private final Map<ResponsivePolicySpec.PolicyType, PolicyPlugin> plugins;

  public ResponsivePolicyReconciler(
      final String environment,
      final ControllerClient controllerClient
  ) {
    this(
        environment,
        new ResponsiveContext(Objects.requireNonNull(controllerClient)),
        ImmutableMap.of(
            PolicyType.KAFKA_STREAMS, new KafkaStreamsPolicyPlugin(environment)
        )
    );
  }

  ResponsivePolicyReconciler(final String environment,
                             final ResponsiveContext responsiveCtx,
                             final Map<ResponsivePolicySpec.PolicyType, PolicyPlugin> plugins) {
    this.environment = environment;
    this.responsiveCtx = Objects.requireNonNull(responsiveCtx);
    this.plugins = Objects.requireNonNull(plugins);
  }

  private Optional<ApplicationState> pollTargetState(final ResponsivePolicy policy) {
    try {
      return Optional.of(responsiveCtx.getControllerClient().getTargetState(
          ControllerProtoFactories.emptyRequest(environment, policy)));
    } catch (final StatusRuntimeException e) {
      if (e.getStatus().getCode().equals(Status.NOT_FOUND.getCode())) {
        LOG.debug("no target state found");
        return Optional.empty();
      }
      throw e;
    }
  }

  private List<ControllerOuterClass.Action> pollActions(final ResponsivePolicy policy) {
    try {
      return responsiveCtx.getControllerClient().getCurrentActions(
          ControllerProtoFactories.emptyRequest(environment, policy)
      );
    } catch (final StatusRuntimeException e) {
      if (e.getStatus().getCode().equals(Status.UNIMPLEMENTED.getCode())) {
        LOG.info("controller has not implemented GetCurrentActions. Return empty list");
        return List.of();
      }
      throw e;
    }
  }

  @Override
  public Map<String, EventSource> prepareEventSources(EventSourceContext<ResponsivePolicy> ctx) {
    final var poller = new PerResourcePollingEventSource<>(
        policy -> {
          try {
            // TODO(rohan): this is a hack to force an event at each poll interval.
            // we should either: 1. make the controller robust to not rely on polling
            //                   2. poll in some less hacky way (while still using events)
            final var targetState = pollTargetState(policy);
            final var actions =  pollActions(policy);
            return ImmutableSet.of(new ActionsWithTimestamp(targetState, actions));
          } catch (final Throwable t) {
            LOG.error("Error fetching target state", t);
            // We return an empty target state to force reconciliation to run, since right now the
            // controller is stateless and relies on periodic updates after it restarts
            return ImmutableSet.of(new ActionsWithTimestamp());
          }
        },
        ctx.getPrimaryCache(),
        10000L,
        ActionsWithTimestamp.class
    );
    final var builder = ImmutableMap.<String, EventSource>builder();
    builder.putAll(EventSourceInitializer.nameEventSources(poller));
    // add the plugin event sources
    // TODO(rohan): how do we make sure that these sources dont cross streams (should be fine
    //              if they are all resource-scoped events)
    builder.putAll(
        plugins.values().stream()
            .map(p -> p.prepareEventSources(ctx, responsiveCtx))
            .flatMap(m -> m.entrySet().stream())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
    );
    return builder.build();
  }

  @Override
  public UpdateControl<ResponsivePolicy> reconcile(final ResponsivePolicy resource,
                                                   final Context<ResponsivePolicy> ctx) {
    LOG.info("Received event for {}", resource.getFullResourceName());
    try {
      resource.getSpec().validate();
    } catch (final RuntimeException e) {
      final var msg = "invalid responsive policy spec: " + e.getMessage();
      LOG.error(msg, e);
      resource.setStatus(new ResponsivePolicyStatus(msg));
      return UpdateControl.patchStatus(resource);
    }
    responsiveCtx.getControllerClient()
        .upsertPolicy(ControllerProtoFactories.upsertPolicyRequest(environment, resource));
    plugins.get(resource.getSpec().getPolicyType()).reconcile(resource, ctx, responsiveCtx);
    return UpdateControl.patchStatus(resource);
  }
}
