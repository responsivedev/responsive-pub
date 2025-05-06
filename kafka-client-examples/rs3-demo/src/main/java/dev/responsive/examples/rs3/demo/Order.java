package dev.responsive.examples.rs3.demo;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * An instance of a single purchase order
 */
public record Order(
    @JsonProperty("orderId") String orderId,
    @JsonProperty("customerId") String customerId,
    @JsonProperty("department") String department,
    @JsonProperty("departmentId") String departmentId,
    @JsonProperty("amount") double amount
) {}