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

package dev.responsive.kafka.internal.license.server.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Request that reports the number of origin events processed
 * by a given (license, thread_id) since the last reported request
 */
public class OriginEventsReportRequestV1 extends LicenseServerRequest {

  public static final String TYPE_NAME = "origin_events_report_v1";

  // transaction id is used to ignore duplicate events if we
  // retry the billing request (UUID)
  private final String transactionId;

  // the number of origin events since the last reported usage event
  private final long eventCount;

  // the customer's environment
  private final String env;

  // the application that reported the usage
  private final String applicationId;

  // the thread that reported the usage
  private final String threadId;

  public OriginEventsReportRequestV1(
      @JsonProperty("type") final String type,
      @JsonProperty("timestamp") final long timestamp,
      @JsonProperty("transactionId") final String transactionId,
      @JsonProperty("eventCount") final long eventCount,
      @JsonProperty("env") final String env,
      @JsonProperty("applicationId") final String applicationId,
      @JsonProperty("threadId") final String threadId
  ) {
    super(type, timestamp);
    this.transactionId = transactionId;
    this.eventCount = eventCount;
    this.env = env;
    this.applicationId = applicationId;
    this.threadId = threadId;
  }

  @JsonProperty("transactionId")
  public String transactionId() {
    return transactionId;
  }

  @JsonProperty("eventCount")
  public long eventCount() {
    return eventCount;
  }

  @JsonProperty("env")
  public String env() {
    return env;
  }

  @JsonProperty("applicationId")
  public String applicationId() {
    return applicationId;
  }

  @JsonProperty("threadId")
  public String threadId() {
    return threadId;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private long timestamp;
    private String transactionId;
    private long eventCount;
    private String env;
    private String applicationId;
    private String threadId;

    public Builder setTimestamp(final long timestamp) {
      this.timestamp = timestamp;
      return this;
    }

    public Builder setTransactionId(final String transactionId) {
      this.transactionId = transactionId;
      return this;
    }

    public Builder setEventCount(final long eventCount) {
      this.eventCount = eventCount;
      return this;
    }

    public Builder setEnv(final String env) {
      this.env = env;
      return this;
    }

    public Builder setApplicationId(final String applicationId) {
      this.applicationId = applicationId;
      return this;
    }

    public Builder setThreadId(final String threadId) {
      this.threadId = threadId;
      return this;
    }

    public OriginEventsReportRequestV1 build() {
      return new OriginEventsReportRequestV1(
          TYPE_NAME,
          timestamp,
          transactionId,
          eventCount,
          env,
          applicationId,
          threadId
      );
    }
  }
}
