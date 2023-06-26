package dev.responsive.k8s.operator.reconciler;

import dev.responsive.k8s.crd.ResponsivePolicy;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.client.dsl.AppsAPIGroupDSL;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import java.util.Map;

public abstract class ManagedApplication {
  public abstract int getReplicas();

  public abstract void setReplicas(final Integer targetReplicas,
                                   final Context<ResponsivePolicy> context);

  public abstract String appType();

  public abstract String getResourceVersion();

  public abstract void maybeAddResponsiveLabel(Context<ResponsivePolicy> ctx,
                                               ResponsivePolicy policy);

  public static ManagedApplication build(Context<ResponsivePolicy> ctx,
                                         ResponsivePolicy policy) {
    if (ctx.getSecondaryResource(Deployment.class).isPresent()) {
      final Deployment deployment = ctx.getSecondaryResource(Deployment.class).get();
      validateLabels(deployment, policy);
      return new ManagedDeployment(deployment, policy.getSpec().getApplicationName(),
          policy.getSpec().getApplicationNamespace());
    } else if (ctx.getSecondaryResource(StatefulSet.class).isPresent()) {
      final StatefulSet statefulSet = ctx.getSecondaryResource(StatefulSet.class).get();
      validateLabels(statefulSet, policy);
      return new ManagedStatefulSet(statefulSet, policy.getSpec().getApplicationName(),
          policy.getSpec().getApplicationNamespace());
    } else {
      // The framework has no associated deployment or StatefulSet yet, which means there is no
      // deployment or StatefulSet with the required label. Label the app here.
      // TODO(rohan): double-check this understanding
      final ManagedApplication managedApplication =  buildFromContext(ctx, policy);
      managedApplication.maybeAddResponsiveLabel(ctx, policy);
      return managedApplication;
    }
  }

  private static void validateLabels(HasMetadata app, ResponsivePolicy policy) {
    final Map<String, String> labels = app.getMetadata().getLabels();
    assert labels.containsKey(ResponsivePolicyReconciler.NAME_LABEL);
    assert
        labels.get(ResponsivePolicyReconciler.NAME_LABEL).equals(policy.getMetadata().getName());
    assert labels.containsKey(ResponsivePolicyReconciler.NAMESPACE_LABEL);
    assert labels.get(ResponsivePolicyReconciler.NAMESPACE_LABEL)
        .equals(policy.getMetadata().getNamespace());
  }

  private static ManagedApplication buildFromContext(Context<ResponsivePolicy> ctx,
                                                    ResponsivePolicy policy) {

    final var appClient =  ctx.getClient().apps();
    final var namespace = policy.getSpec().getApplicationNamespace();
    final var appName = policy.getSpec().getApplicationName();

    if (!(isDeployment(appClient, namespace, appName)
        || isStatefulSet(appClient, namespace, appName))) {
      throw new RuntimeException(String.format("No deployment or StatefulSet %s/%s found",
          namespace, appName));
    }

    if (isDeployment(appClient, namespace, appName)) {
      final Deployment deployment = appClient.deployments()
          .inNamespace(namespace)
          .withName(appName)
          .get();
      return new ManagedDeployment(deployment, appName, namespace);
    }

    final StatefulSet statefulSet = appClient.statefulSets()
        .inNamespace(namespace)
        .withName(appName)
        .get();

    return new ManagedStatefulSet(statefulSet, appName, namespace);
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
