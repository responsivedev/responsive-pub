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

import dev.responsive.k8s.crd.ResponsivePolicy;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import java.util.HashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ManagedStatefulSet extends ManagedApplication {

  private static final Logger LOG = LoggerFactory.getLogger(ManagedStatefulSet.class);

  private final StatefulSet statefulSet;
  private final String appName;
  private  final String namespace;

  public ManagedStatefulSet(final StatefulSet statefulSet, final String appName,
                           final String namespace) {
    this.statefulSet = statefulSet;
    this.appName = appName;
    this.namespace = namespace;
  }

  @Override
  public int getReplicas() {
    return statefulSet.getSpec().getReplicas();
  }

  @Override
  public void setReplicas(final Integer targetReplicas, final Context<ResponsivePolicy> context) {
    context.getClient().apps().statefulSets()
        .inNamespace(namespace)
        .withName(appName)
        .edit(d -> {
          if (d.getMetadata().getResourceVersion()
              .equals(getResourceVersion())) {
            d.getSpec().setReplicas(targetReplicas);
          }
          return d;
        });
  }

  @Override
  public void restartPod(final String podId, final Context<ResponsivePolicy> context) {
    final var sts = context.getClient().apps().statefulSets()
        .inNamespace(namespace)
        .withName(appName);
    final var podSelector = sts.get().getSpec().getSelector();
    findPod(podId, context, namespace, podSelector).delete();
  }

  @Override
  public String appType() {
    return "StatefulSet";
  }

  public String getResourceVersion() {
    return statefulSet.getMetadata().getResourceVersion();
  }

  @Override
  public void maybeAddResponsiveLabel(Context<ResponsivePolicy> ctx, ResponsivePolicy policy) {
    final boolean hasNameLabel
        = statefulSet.getMetadata().getLabels().containsKey(ResponsivePolicyReconciler.NAME_LABEL);
    final boolean hasNamespaceLabel
        = statefulSet.getMetadata().getLabels()
        .containsKey(ResponsivePolicyReconciler.NAMESPACE_LABEL);
    if (!hasNameLabel || !hasNamespaceLabel) {
      // TODO(rohan): I don't think this is patching the way I expect. Review the patch APIs
      //  things to check: do we need to check the version or does that happen automatically?
      //  things to check: this should only be updating the labels and thats it (e.g. truly
      //  a patch)
      ctx.getClient().apps().statefulSets()
          .inNamespace(statefulSet.getMetadata().getNamespace())
          .withName(statefulSet.getMetadata().getName())
          .edit(d -> {
            if (d.getMetadata().getResourceVersion()
                .equals(statefulSet.getMetadata().getResourceVersion())) {
              final HashMap<String, String> newLabels =
                  new HashMap<>(d.getMetadata().getLabels());
              newLabels.put(
                  ResponsivePolicyReconciler.NAMESPACE_LABEL,
                  policy.getMetadata().getNamespace()
              );
              newLabels.put(ResponsivePolicyReconciler.NAME_LABEL,
                  policy.getMetadata().getName());
              d.getMetadata().setLabels(newLabels);
            }
            LOG.info("Added labels '{}:{}' and '{}:{}' to statefulset {}",
                ResponsivePolicyReconciler.NAME_LABEL,
                policy.getMetadata().getName(),
                ResponsivePolicyReconciler.NAMESPACE_LABEL,
                policy.getMetadata().getNamespace(),
                statefulSet.getMetadata().getName());
            return d;
          });
    }
  }
}
