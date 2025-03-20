package dev.responsive.kafka.internal.db.rs3.client.grpc;

import dev.responsive.kafka.internal.db.rs3.client.RangeBound;
import dev.responsive.rs3.Rs3;
import io.grpc.stub.StreamObserver;

public interface GrpcRangeRequestProxy {
  /**
   *
   * @param start
   * @param resultObserver
   */
  void send(RangeBound start, StreamObserver<Rs3.RangeResult> resultObserver);
}
