package dev.responsive.k8s.crd;

import java.util.Optional;

public final class CrdUtils {
  private CrdUtils() {
  }

  public static <T> T validatePresent(final Optional<T> o, final String name) {
    return o.orElseThrow(
        () -> new RuntimeException(String.format("value %s not present", name)));
  }
}
