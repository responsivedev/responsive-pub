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

package dev.responsive.reconciler;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import dev.responsive.controller.ControllerClient;
import dev.responsive.controller.ControllerProtoFactories;
import dev.responsive.k8s.crd.ResponsivePolicy;
import dev.responsive.k8s.crd.ResponsivePolicySpec;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.config.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.processing.event.source.EventSource;
import io.javaoperatorsdk.operator.processing.event.source.polling.PerResourcePollingEventSource;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ResponsivePolicyReconcilerTest {
  @Mock
  private Context<ResponsivePolicy> ctx;
  @Mock
  private EventSourceContext<ResponsivePolicy> eCtx;
  @Mock
  private ControllerConfiguration<ResponsivePolicy> controllerConfig;
  @Mock
  private KubernetesClient client;
  @Mock
  private ControllerClient controllerClient;
  @Mock
  private PolicyPlugin plugin;
  @Mock
  private EventSource pluginEventSource1;
  @Mock
  private EventSource pluginEventSoruce2;

  private ResponsivePolicyReconciler reconciler;
  private ResponsiveContext rCtx;
  private final ResponsivePolicy policy = new ResponsivePolicy();

  @BeforeEach
  public void setup() {
    rCtx = new ResponsiveContext(controllerClient);
    lenient().when(eCtx.getControllerConfiguration()).thenReturn(controllerConfig);
    lenient().when(controllerConfig.getEffectiveNamespaces()).thenReturn(ImmutableSet.of("responsive"));
    lenient().when(eCtx.getClient()).thenReturn(client);
    lenient().when(plugin.prepareEventSources(eCtx, rCtx)).thenReturn(
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
        ResponsivePolicySpec.PolicyType.DEMO,
        Optional.of(new ResponsivePolicySpec.DemoPolicy(123))
    ));
    reconciler = new ResponsivePolicyReconciler(
        rCtx,
        ImmutableMap.of(ResponsivePolicySpec.PolicyType.DEMO, plugin)
    );
  }

  @Test
  public void shouldIncludeControllerPollingEventSource() {
    // when:
    final var sources = reconciler.prepareEventSources(eCtx);

    // then:
    assertThat(sources.values(), hasItem(instanceOf(PerResourcePollingEventSource.class)));
  }

  @Test
  public void shouldIncludePluginEventSources() {
    // when:
    final var sources = reconciler.prepareEventSources(eCtx);
    assertThat(sources, hasEntry("pes1", pluginEventSource1));
    assertThat(sources, hasEntry("pes2", pluginEventSoruce2));
  }

  @Test
  public void shouldUpdateCurrentPolicySpec() {
    // when:
    reconciler.reconcile(policy, ctx);

    // then:
    verify(controllerClient).upsertPolicy(ControllerProtoFactories.upsertPolicyRequest(policy));
  }

  @Test
  public void shouldDispatchToCorrectPolicy() {
    // when:
    reconciler.reconcile(policy, ctx);

    // then:
    verify(plugin).reconcile(policy, ctx, rCtx);
  }
}