/*
 * Copyright 2024 Responsive Computing, Inc.
 *
 * This source code is licensed under the Responsive Business Source License Agreement v1.0
 * available at:
 *
 * https://www.responsive.dev/legal/responsive-bsl-10
 *
 * This software requires a valid Commercial License Key for production use. Trial and commercial
 * licenses can be obtained at https://www.responsive.dev
 */

package dev.responsive.k8s.crd.kafkastreams;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class FixedReplicaScaleUpStrategySpec {
  private final int replicas;

  @JsonCreator
  public FixedReplicaScaleUpStrategySpec(@JsonProperty("replicas") final int replicas) {
    this.replicas = replicas;
  }

  public int getReplicas() {
    return replicas;
  }
}
