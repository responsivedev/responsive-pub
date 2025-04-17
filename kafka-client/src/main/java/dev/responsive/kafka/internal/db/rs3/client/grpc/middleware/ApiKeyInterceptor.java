package dev.responsive.kafka.internal.db.rs3.client.grpc.middleware;

import dev.responsive.kafka.internal.db.rs3.client.grpc.ApiCredential;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import java.util.Objects;

public class ApiKeyInterceptor implements ClientInterceptor {
  private final ApiCredential credential;

  public ApiKeyInterceptor(final ApiCredential credential) {
    this.credential = Objects.requireNonNull(credential);
  }

  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
      final MethodDescriptor<ReqT, RespT> method,
      final CallOptions callOptions,
      final Channel next
  ) {
    return new ForwardingClientCall.SimpleForwardingClientCall<>(
        next.newCall(method, callOptions)
    ) {
      @Override
      public void start(Listener<RespT> responseListener, Metadata headers) {
        credential.attachToRequest(headers);
        super.start(responseListener, headers);
      }
    };
  }
}
