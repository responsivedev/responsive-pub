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
  // TODO: dont use the protobuf enum type in the k8s crd definition
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
    this.applicationNamespace = applicationNamespace;
    this.applicationName = applicationName;
    this.status = status;
    this.policyType = policyType;
    this.demoPolicy = Objects.requireNonNull(demoPolicy);
  }

  public void validate() {
    Objects.requireNonNull(applicationName, "applicationName");
    Objects.requireNonNull(applicationNamespace, "applicationNamespace");
    Objects.requireNonNull(status, "status");
    Objects.requireNonNull(policyType, "policyType");
    switch (policyType) {
      case DEMO:
        CrdUtils.validatePresent(demoPolicy, "demoPolicy").validate();
        break;
      default:
        break;
    }
  }

  public String getApplicationNamespace() {
    return applicationNamespace;
  }

  public String getApplicationName() {
    return applicationName;
  }

  public PolicyStatus getStatus() {
    return status;
  }

  public PolicyType getPolicyType() {
    return policyType;
  }

  public Optional<DemoPolicy> getDemoPolicy() {
    return demoPolicy;
  }

}