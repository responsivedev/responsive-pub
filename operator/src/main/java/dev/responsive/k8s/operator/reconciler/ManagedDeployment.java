package dev.responsive.k8s.operator.reconciler;

import dev.responsive.k8s.crd.ResponsivePolicy;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import java.util.HashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ManagedDeployment extends ManagedApplication {

  private static final Logger LOG = LoggerFactory.getLogger(ManagedDeployment.class);
  private final Deployment deployment;
  private final String appName;
  private  final String namespace;

  public ManagedDeployment(final Deployment deployment, final String appName,
                           final String namespace) {
    this.deployment = deployment;
    this.appName = appName;
    this.namespace = namespace;
  }

  @Override
  public int getReplicas() {
    return deployment.getSpec().getReplicas();
  }

  @Override
  public void setReplicas(final Integer targetReplicas, final Context<ResponsivePolicy> context) {
    context.getClient().apps().deployments()
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
  public String appType() {
    return "Deployment";
  }

  @Override
  public String getResourceVersion() {
    return deployment.getMetadata().getResourceVersion();
  }

  @Override
  public void maybeAddResponsiveLabel(Context<ResponsivePolicy> ctx, ResponsivePolicy policy) {
    final boolean hasNameLabel
        = deployment.getMetadata().getLabels().containsKey(ResponsivePolicyReconciler.NAME_LABEL);
    final boolean hasNamespaceLabel
        = deployment.getMetadata().getLabels()
        .containsKey(ResponsivePolicyReconciler.NAMESPACE_LABEL);
    if (!hasNameLabel || !hasNamespaceLabel) {
      // TODO(rohan): I don't think this is patching the way I expect. Review the patch APIs
      //  things to check: do we need to check the version or does that happen automatically?
      //  things to check: this should only be updating the labels and thats it (e.g. truly
      //  a patch)
      ctx.getClient().apps().deployments()
          .inNamespace(deployment.getMetadata().getNamespace())
          .withName(deployment.getMetadata().getName())
          .edit(d -> {
            if (d.getMetadata().getResourceVersion()
                .equals(deployment.getMetadata().getResourceVersion())) {
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
            LOG.info("Added labels '{}:{}' and '{}:{}' to deployment {}",
                ResponsivePolicyReconciler.NAME_LABEL,
                policy.getMetadata().getName(),
                ResponsivePolicyReconciler.NAMESPACE_LABEL,
                policy.getMetadata().getNamespace(),
                deployment.getMetadata().getName());
            return d;
          });
    }
  }

}
