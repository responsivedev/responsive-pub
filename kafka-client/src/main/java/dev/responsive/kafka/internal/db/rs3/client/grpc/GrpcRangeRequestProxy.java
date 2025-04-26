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



package dev.responsive.kafka.internal.db.rs3.client.grpc;

import dev.responsive.kafka.internal.db.rs3.client.Range;
import dev.responsive.rs3.Rs3;
import io.grpc.stub.StreamObserver;

/**
 * Helper class for sending and retrying Range requests to RS3. As new key-values
 * are observed through the `StreamObserver`, the start of the bound should be
 * updated. If the observer encounters an error, then it can retry with the updated
 * start bound.
 */
public interface GrpcRangeRequestProxy<K extends Comparable<K>> {
  /**
   * Send a range request with an updated start bound. The results will be passed
   * through to result observer. If a transient error is encountered through the
   * observer, the caller can retry this operation with an updated `startBound`.
   *
   * @param range The updated range based on key-values seen with `resultObserver`
   * @param resultObserver An observer for key-value results and the end of stream marker
   */
  void send(Range<K> range, StreamObserver<Rs3.RangeResult> resultObserver);

}
