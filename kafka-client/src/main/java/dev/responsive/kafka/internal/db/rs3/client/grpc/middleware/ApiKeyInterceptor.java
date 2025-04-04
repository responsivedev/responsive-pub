package dev.responsive.kafka.internal.db.rs3.client.grpc.middleware;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Objects;

public class ApiKeyInterceptor implements ClientInterceptor {
  private static final String API_KEY_KEY_STR = "responsive-api-key";
  private static final Metadata.Key<String> API_KEY_KEY
      = Metadata.Key.of(API_KEY_KEY_STR, Metadata.ASCII_STRING_MARSHALLER);

  private final String encodedApiKey;

  public ApiKeyInterceptor(final String apiKey) {
    this.encodedApiKey = Objects.requireNonNull(
        Base64.getEncoder().encodeToString(apiKey.getBytes(StandardCharsets.UTF_8)));
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
        headers.put(API_KEY_KEY, encodedApiKey);
        super.start(responseListener, headers);
      }
    };
  }
}
