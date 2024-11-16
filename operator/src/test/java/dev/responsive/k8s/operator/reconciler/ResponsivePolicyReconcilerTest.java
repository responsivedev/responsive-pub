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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import dev.responsive.controller.client.ControllerClient;
import dev.responsive.k8s.controller.ControllerProtoFactories;
import dev.responsive.k8s.crd.ResponsivePolicy;
import dev.responsive.k8s.crd.ResponsivePolicySpec;
import dev.responsive.k8s.crd.ResponsivePolicySpec.PolicyType;
import dev.responsive.k8s.crd.kafkastreams.DemoPolicySpec;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.config.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import io.javaoperatorsdk.operator.processing.event.source.EventSource;
import io.javaoperatorsdk.operator.processing.event.source.IndexerResourceCache;
import io.javaoperatorsdk.operator.processing.event.source.polling.PerResourcePollingEventSource;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import responsive.controller.v1.controller.proto.ControllerOuterClass.PolicyStatus;

@ExtendWith(MockitoExtension.class)
class ResponsivePolicyReconcilerTest {
  @Mock
  private Context<ResponsivePolicy> ctx;
  @Mock
  private IndexerResourceCache<ResponsivePolicy> cache;
  @Mock
  private EventSourceContext<ResponsivePolicy> eventCtx;
  @Mock
  private ControllerConfiguration<ResponsivePolicy> controllerConfig;
  @Mock
  private KubernetesClient client;
  @Mock
  private ControllerClient controllerClient;
  @Mock
  private dev.responsive.k8s.operator.reconciler.PolicyPlugin plugin;
  @Mock
  private EventSource pluginEventSource1;
  @Mock
  private EventSource pluginEventSoruce2;

  private dev.responsive.k8s.operator.reconciler.ResponsivePolicyReconciler reconciler;
  private dev.responsive.k8s.operator.reconciler.ResponsiveContext responsiveCtx;
  private final ResponsivePolicy policy = new ResponsivePolicy();

  @BeforeEach
  public void setup() {
    responsiveCtx = new dev.responsive.k8s.operator.reconciler.ResponsiveContext(controllerClient);
    lenient().when(eventCtx.getControllerConfiguration()).thenReturn(controllerConfig);
    lenient().when(controllerConfig.getEffectiveNamespaces())
        .thenReturn(ImmutableSet.of("responsive"));
    lenient().when(eventCtx.getClient()).thenReturn(client);
    lenient().when(eventCtx.getPrimaryCache()).thenReturn(cache);
    lenient().when(plugin.prepareEventSources(eventCtx, responsiveCtx)).thenReturn(
        ImmutableMap.of(
            "pes1", pluginEventSource1,
            "pes2", pluginEventSoruce2
        )
    );
    policy.setMetadata(new ObjectMeta());
    policy.getMetadata().setNamespace("foo");
    policy.getMetadata().setName("bar");
    policy.setSpec(new ResponsivePolicySpec(
        "ping",
        "pong",
        "pop",
        PolicyStatus.POLICY_STATUS_MANAGED,
        ResponsivePolicySpec.PolicyType.DEMO,
        Optional.of(new DemoPolicySpec(123, 7, 1, Optional.empty(), Optional.empty())),
        Optional.empty()
    ));
    reconciler = new dev.responsive.k8s.operator.reconciler.ResponsivePolicyReconciler(
        "testenv",
        responsiveCtx,
        ImmutableMap.of(PolicyType.KAFKA_STREAMS, plugin)
    );
  }

  @Test
  public void shouldIncludeControllerPollingEventSource() {
    // when:
    final var sources = reconciler.prepareEventSources(eventCtx);

    // then:
    assertThat(sources.values(), hasItem(instanceOf(PerResourcePollingEventSource.class)));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldReturnEmptyTargetStateIfControllerPollFails() {
    // given:
    final var sources = reconciler.prepareEventSources(eventCtx);
    final var maybeSource = sources.values().stream()
        .filter(s -> s instanceof PerResourcePollingEventSource)
        .findFirst();
    assertThat(maybeSource, not(Optional.empty()));
    final var source = (PerResourcePollingEventSource) maybeSource.get();
    final var resource = mock(ResponsivePolicy.class);
    when(resource.getMetadata()).thenReturn(new ObjectMeta());
    when(resource.getSpec()).thenReturn(new ResponsivePolicySpec(
        "ping",
        "pong",
        "pop",
        PolicyStatus.POLICY_STATUS_MANAGED,
        ResponsivePolicySpec.PolicyType.DEMO,
        Optional.of(new DemoPolicySpec(123, 10, 1, Optional.empty(), Optional.empty())),
        Optional.empty()
    ));
    when(controllerClient.getTargetState(any())).thenThrow(new RuntimeException("oops"));

    // when:
    final var ret = (Optional<ActionsWithTimestamp>) source.getSecondaryResource(resource);

    // then:
    assertThat(ret.isPresent(), is(true));
    assertThat(ret.get().getTargetState().isPresent(), is(false));
  }

  @Test
  public void shouldIncludePluginEventSources() {
    // when:
    final var sources = reconciler.prepareEventSources(eventCtx);
    assertThat(sources, hasEntry("pes1", pluginEventSource1));
    assertThat(sources, hasEntry("pes2", pluginEventSoruce2));
  }

  @Test
  public void shouldUpdateCurrentPolicySpec() {
    // when:
    reconciler.reconcile(policy, ctx);

    // then:
    verify(controllerClient)
        .upsertPolicy(ControllerProtoFactories.upsertPolicyRequest("testenv", policy));
  }

  @Test
  public void shouldDispatchToCorrectPolicy() {
    // when:
    reconciler.reconcile(policy, ctx);

    // then:
    verify(plugin).reconcile(policy, ctx, responsiveCtx);
  }

  @Test
  public void shouldValidatePolicy() {
    // given:
    policy.setSpec(new ResponsivePolicySpec(
        "foo",
        null,
        "bar",
        PolicyStatus.POLICY_STATUS_MANAGED,
        PolicyType.DEMO,
        Optional.of(new DemoPolicySpec(10, 0, 1, Optional.empty(), Optional.empty())),
        Optional.empty()
    ));

    // when:
    final UpdateControl<ResponsivePolicy> result = reconciler.reconcile(policy, ctx);

    // then:
    assertThat(
        result.getResource().getStatus().getMessage(),
        startsWith("invalid responsive policy spec")
    );
    verifyNoInteractions(plugin);
  }
}
