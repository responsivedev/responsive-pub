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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import dev.responsive.controller.client.ControllerClient;
import dev.responsive.k8s.controller.ControllerProtoFactories;
import dev.responsive.k8s.crd.ResponsivePolicy;
import dev.responsive.k8s.crd.ResponsivePolicySpec;
import dev.responsive.k8s.crd.ResponsivePolicyStatus;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceInitializer;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import io.javaoperatorsdk.operator.processing.event.source.EventSource;
import io.javaoperatorsdk.operator.processing.event.source.polling.PerResourcePollingEventSource;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        ImmutableMap.of(ResponsivePolicySpec.PolicyType.DEMO, new DemoPolicyPlugin(environment))
    );
  }

  ResponsivePolicyReconciler(final String environment,
                             final ResponsiveContext responsiveCtx,
                             final Map<ResponsivePolicySpec.PolicyType, PolicyPlugin> plugins) {
    this.environment = environment;
    this.responsiveCtx = Objects.requireNonNull(responsiveCtx);
    this.plugins = Objects.requireNonNull(plugins);
  }

  @Override
  public Map<String, EventSource> prepareEventSources(EventSourceContext<ResponsivePolicy> ctx) {
    final var poller = new PerResourcePollingEventSource<>(
        policy -> {
          try {
            return ImmutableSet.of(new TargetStateWithTimestamp(
                // TODO(rohan): this is a hack to force an event at each poll interval.
                // we should either: 1. make the controller robust to not rely on polling
                //                   2. poll in some less hacky way (while still using events)
                responsiveCtx.getControllerClient()
                    .getTargetState(ControllerProtoFactories.emptyRequest(environment, policy))));
          } catch (final Throwable t) {
            LOG.error("Error fetching target state", t);
            // We return an empty target state to force reconciliation to run, since right now the
            // controller is stateless and relies on periodic updates after it restarts
            return ImmutableSet.of(new TargetStateWithTimestamp());
          }
        },
        ctx.getPrimaryCache(),
        10000L,
        TargetStateWithTimestamp.class
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
