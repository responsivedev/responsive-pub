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

package dev.responsive.k8s.crd;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;
import java.util.Optional;
import responsive.controller.v1.controller.proto.ControllerOuterClass.PolicyStatus;

public class ResponsivePolicySpec {
  private final String applicationNamespace;
  private final String applicationName;
  private final PolicyStatus status;
  private final ResponsivePolicySpec.PolicyType policyType;
  private final Optional<DemoPolicy> demoPolicy;

  public enum PolicyType {
    DEMO
  }

  @JsonCreator
  public ResponsivePolicySpec(
      @JsonProperty("applicationNamespace") final String applicationNamespace,
      @JsonProperty("applicationName") final String applicationName,
      @JsonProperty("status") final PolicyStatus status,
      @JsonProperty("policyType") final PolicyType policyType,
      @JsonProperty("demoPolicy") final Optional<DemoPolicy> demoPolicy
  ) {
    this.applicationNamespace = Objects.requireNonNull(applicationNamespace);
    this.applicationName = Objects.requireNonNull(applicationName);
    this.status = Objects.requireNonNull(status);
    this.policyType = policyType;
    this.demoPolicy = Objects.requireNonNull(demoPolicy);
  }

  public String applicationNamespace() {
    return applicationNamespace;
  }

  public String applicationName() {
    return applicationName;
  }

  public PolicyStatus status() {
    return status;
  }

  public PolicyType policyType() {
    return policyType;
  }

  public Optional<DemoPolicy> demoPolicy() {
    return demoPolicy;
  }

  public static class DemoPolicy {

    private final int maxReplicas;

    @JsonCreator
    public DemoPolicy(
        @JsonProperty("maxReplicas") final int maxReplicas
    ) {
      this.maxReplicas = maxReplicas;
    }

    public int maxReplicas() {
      return maxReplicas;
    }
  }
}