package dev.responsive.kafka.internal.db.rs3.client.grpc;

import io.grpc.Metadata;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

public interface ApiCredential {
  void attachToRequest(final Metadata headers);

  static ApiCredential forApiKey(final String apiKey) {
    return new ApiKey(apiKey);
  }

  static ApiCredential forInternalKey(final String internalKey) {
    return new InternalKey(internalKey);
  }

  class ApiKey implements ApiCredential {
    private static final String API_KEY_KEY_STR = "responsive-api-key";
    private static final Metadata.Key<String> API_KEY_KEY
        = Metadata.Key.of(API_KEY_KEY_STR, Metadata.ASCII_STRING_MARSHALLER);

    private final String encodedApiKey;

    private ApiKey(final String apiKey) {
      this.encodedApiKey = Base64.getEncoder()
          .encodeToString(apiKey.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public void attachToRequest(final Metadata headers) {
      headers.put(API_KEY_KEY, encodedApiKey);
    }
  }

  class InternalKey implements ApiCredential {
    private static final String INTERNAL_KEY_STR = "responsive-internal-api-key";
    private static final Metadata.Key<String> INTERNAL_KEY_KEY
        = Metadata.Key.of(INTERNAL_KEY_STR, Metadata.ASCII_STRING_MARSHALLER);

    private final String encodedInternalKey;

    private InternalKey(String internalKey) {
      this.encodedInternalKey = Base64.getEncoder()
          .encodeToString(internalKey.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public void attachToRequest(final Metadata headers) {
      headers.put(INTERNAL_KEY_KEY, encodedInternalKey);
    }
  }
}
