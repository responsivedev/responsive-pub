package dev.responsive.examples.rs3.demo;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.HashMap;

public record CustomerOrderTracker(
    @JsonProperty("customerId") String customerId,
    @JsonProperty("totalSpend") double totalSpend,
    @JsonProperty("totalSpendByDepartment") HashMap<String, Double> totalSpendByDepartment
) {}
