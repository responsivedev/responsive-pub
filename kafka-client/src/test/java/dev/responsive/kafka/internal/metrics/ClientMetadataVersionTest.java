/*
 * Copyright 2024 Responsive Computing, Inc.
 *
 * This source code is licensed under the Responsive Software License Agreement v1.0 available at
 *
 * https://www.responsive.dev/legal/responsive-bsl-10
 *
 * This software requires a valid Commercial License Key for production use. Trial and commercial
 * licenses can be obtained at https://www.responsive.dev/sdk/get-started.
 */

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
