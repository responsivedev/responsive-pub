package dev.responsive.kafka.internal.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.responsive.kafka.internal.license.model.LicenseDocumentV1;
import dev.responsive.kafka.internal.license.model.TimedTrialV1;
import java.io.IOException;
import java.time.Instant;
import java.util.Base64;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.MessageType;
import software.amazon.awssdk.services.kms.model.SignRequest;
import software.amazon.awssdk.services.kms.model.SignResponse;
import software.amazon.awssdk.services.kms.model.SigningAlgorithmSpec;

public class GenerateTrialLicense {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String SIGNING_KEY_ARN = System.getenv("SIGNING_KEY_ARN");
  private static final String SIGNING_KEY_ID = System.getenv("SIGNING_KEY_ID");
  private static final String AWS_PROFILE = System.getenv("AWS_PROFILE");
  private static final String SIGNING_ALGO = "RSASSA_PSS_SHA_256";

  public static void main(final String[] args) throws IOException {
    final var info = new TimedTrialV1(
        "timed_trial_v1",
        "test@responsive.dev",
        Instant.now().getEpochSecond(),
        Instant.MAX.getEpochSecond()
    );
    final byte[] serialized = MAPPER.writeValueAsBytes(info);
    final Region region = Region.US_WEST_2;
    final byte[] signature;
    try (final KmsClient kmsClient = KmsClient.builder()
        .credentialsProvider(ProfileCredentialsProvider.create(AWS_PROFILE))
        .region(region)
        .build()
    ) {
      final SdkBytes sdkBytes = SdkBytes.fromByteArray(serialized);
      final SignRequest signRequest = SignRequest.builder()
          .keyId(SIGNING_KEY_ARN)
          .message(sdkBytes)
          .messageType(MessageType.RAW)
          .signingAlgorithm(SigningAlgorithmSpec.RSASSA_PSS_SHA_256)
          .build();
      final SignResponse signResponse = kmsClient.sign(signRequest);
      final SdkBytes sdkSignature = signResponse.signature();
      signature = sdkSignature.asByteArray();
    }
    final LicenseDocumentV1 license = new LicenseDocumentV1(
        "1",
        Base64.getEncoder().encodeToString(serialized),
        Base64.getEncoder().encodeToString(signature),
        SIGNING_KEY_ID,
        SIGNING_ALGO
    );
    System.out.println(MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(license));
  }
}
