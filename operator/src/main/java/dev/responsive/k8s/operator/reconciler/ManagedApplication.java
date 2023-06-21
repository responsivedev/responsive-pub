package dev.responsive.k8s.operator.reconciler;

import dev.responsive.k8s.crd.ResponsivePolicy;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.client.dsl.AppsAPIGroupDSL;
import io.javaoperatorsdk.operator.api.reconciler.Context;

import java.util.HashMap;
import java.util.Map;

public class ManagedApplication {

  private final HasMetadata application;
  private final Class appClass;
  public ManagedApplication(HasMetadata application, Class appClass) {
    this.application = application;
    this.appClass = appClass;
  }

  public int getReplicas() {
    if (appClass == Deployment.class) {
      return ((Deployment) application).getSpec().getReplicas();
    } else if (appClass == StatefulSet.class) {
      return ((StatefulSet) application).getSpec().getReplicas();
    }

    throw new RuntimeException(
        String.format("Expecting app to be either a Deployment or StatefulSet. It was %s",
            appClass.toString()));
  }


  public String getResourceVersion() {
    if (appClass == Deployment.class) {
      return ((Deployment) application).getMetadata().getResourceVersion();
    } else if (appClass == StatefulSet.class) {
      return ((StatefulSet) application).getMetadata().getResourceVersion();
    }

    throw new RuntimeException(
        String.format("Expecting app to be either a Deployment or StatefulSet. It was %s",
            appClass.toString()));
  }

  public static void validateLabels(HasMetadata app, ResponsivePolicy policy) {
    final Map<String, String> labels = app.getMetadata().getLabels();
    assert labels.containsKey(ResponsivePolicyReconciler.NAME_LABEL);
    assert
        labels.get(ResponsivePolicyReconciler.NAME_LABEL).equals(policy.getMetadata().getName());
    assert labels.containsKey(ResponsivePolicyReconciler.NAMESPACE_LABEL);
    assert labels.get(ResponsivePolicyReconciler.NAMESPACE_LABEL)
        .equals(policy.getMetadata().getNamespace());
  }

  public static ManagedApplication buildFromContext(Context<ResponsivePolicy> ctx,
                                                    ResponsivePolicy policy) {

    final var appClient =  ctx.getClient().apps();
    final var namespace = policy.getSpec().getApplicationNamespace();
    final var appName = policy.getSpec().getApplicationName();

    if (!(isDeployment(appClient, namespace, appName)
        || isStatefulSet(appClient, namespace, appName))) {
      throw new RuntimeException(String.format("No deployment %s/%s found", namespace, appName));
    }

    if (isDeployment(appClient, namespace, appName)) {
      final Deployment deployment = appClient.deployments()
          .inNamespace(namespace)
          .withName(appName)
          .get();
      return new ManagedApplication(deployment, Deployment.class);
    }

    final StatefulSet statefulSet = appClient.statefulSets()
        .inNamespace(namespace)
        .withName(appName)
        .get();

    return new ManagedApplication(statefulSet, StatefulSet.class);

  }

  private void maybeAddResponsiveLabel(
      final HasMetadata deployment,
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

  private static boolean isDeployment(AppsAPIGroupDSL appClient, String namespace, String appName) {
    final long matches = appClient.deployments()
        .inNamespace(namespace)
        .list()
        .getItems()
        .stream()
        .filter((deployment) -> deployment.getMetadata().getName().equals(appName))
        .count();

    assert (matches <= 1);
    return matches == 1;
  }

  private static boolean isStatefulSet(AppsAPIGroupDSL appClient, String namespace,
                                       String appName) {
    final long matches = appClient.statefulSets()
        .inNamespace(namespace)
        .list()
        .getItems()
        .stream()
        .filter((statefulSet) -> statefulSet.getMetadata().getName().equals(appName))
        .count();

    assert (matches <= 1);
    return matches == 1;
  }
}
