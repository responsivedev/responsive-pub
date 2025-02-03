package dev.responsive.kafka.internal.db.rs3.client.grpc.middleware;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import java.util.Objects;
import java.util.UUID;

public class PssHeadersInterceptor implements ClientInterceptor {
  private static final String STORE_ID = "store-id";
  private static final Metadata.Key<String> STORE_ID_KEY
      = Metadata.Key.of(STORE_ID, Metadata.ASCII_STRING_MARSHALLER);
  private static final String PSS_ID = "pss-id";
  private static final Metadata.Key<String> PSS_ID_KEY
      = Metadata.Key.of(PSS_ID, Metadata.ASCII_STRING_MARSHALLER);

  private final UUID storeId;
  private final int pssId;

  public PssHeadersInterceptor(final UUID storeId, final int pssId) {
    this.storeId = Objects.requireNonNull(storeId);
    this.pssId = pssId;
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
        headers.put(STORE_ID_KEY, storeId.toString());
        headers.put(PSS_ID_KEY, Integer.toString(pssId));
        super.start(responseListener, headers);
      }
    };
  }
}
