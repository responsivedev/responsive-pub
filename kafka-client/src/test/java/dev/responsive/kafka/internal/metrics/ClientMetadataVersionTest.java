package dev.responsive.kafka.internal.metrics;

import static org.junit.jupiter.api.Assertions.assertFalse;

import org.junit.jupiter.api.Test;

public class ClientMetadataVersionTest {

  @Test
  public void shouldParseVersionProperties() {
    final ClientVersionMetadata versionMetadata = ClientVersionMetadata.loadVersionMetadata();
    System.out.println(versionMetadata);
    assertFalse(versionMetadata.responsiveClientVersion.isEmpty());
    assertFalse(versionMetadata.responsiveClientCommitId.isEmpty());
    assertFalse(versionMetadata.streamsClientVersion.isEmpty());
    assertFalse(versionMetadata.streamsClientCommitId.isEmpty());
  }

}
