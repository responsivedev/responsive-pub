/*
 * Copyright 2025 Responsive Computing, Inc.
 *
 * This source code is licensed under the Responsive Business Source License Agreement v1.0
 * available at:
 *
 * https://www.responsive.dev/legal/responsive-bsl-10
 *
 * This software requires a valid Commercial License Key for production use. Trial and commercial
 * licenses can be obtained at https://www.responsive.dev
 */

package dev.responsive.examples.regression.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.HashMap;

public record CustomerOrderTracker(
    @JsonProperty("customerId") String customerId,
    @JsonProperty("totalSpend") double totalSpend,
    @JsonProperty("totalSpendByDepartment") HashMap<String, Double> totalSpendByDepartment
) {}
