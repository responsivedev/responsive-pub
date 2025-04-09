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

package dev.responsive.kafka.internal.db.rs3.client;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.when;

import dev.responsive.kafka.internal.db.rs3.PssPartitioner;
import dev.responsive.kafka.internal.stores.ResponsiveStoreRegistration;
import java.util.Arrays;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class RS3ClientUtilTest {

  @Mock
  private RS3Client client;

  @Mock
  private PssPartitioner partitioner;


  @Test
  public void shouldReturnLastWrittenOffset() {
    // Given:
    final var storeId = UUID.randomUUID();
    final var lssId = new LssId(0);
    when(partitioner.pssForLss(lssId))
        .thenReturn(Arrays.asList(0, 1, 2));

    // When:
    when(client.getCurrentOffsets(storeId, lssId, 0))
        .thenReturn(new CurrentOffsets(Optional.of(15L), Optional.of(10L)));
    when(client.getCurrentOffsets(storeId, lssId, 1))
        .thenReturn(new CurrentOffsets(Optional.of(20L), Optional.of(5L)));
    when(client.getCurrentOffsets(storeId, lssId, 2))
        .thenReturn(new CurrentOffsets(Optional.empty(), Optional.empty()));

    // Then:
    final var clientUtil = new RS3ClientUtil(storeId, client, partitioner);
    final var lssMetadata = clientUtil.fetchLssMetadata(lssId);
    assertThat(lssMetadata.lastWrittenOffset(), is(20L));
    assertThat(lssMetadata.writtenOffsets().get(0), is(Optional.of(15L)));
    assertThat(lssMetadata.writtenOffsets().get(1), is(Optional.of(20L)));
    assertThat(lssMetadata.writtenOffsets().get(2), is(Optional.empty()));
  }

  @Test
  public void shouldReturnMinusOneForLastWrittenOffset() {
    // Given:
    final var storeId = UUID.randomUUID();
    final var lssId = new LssId(0);
    when(partitioner.pssForLss(lssId))
        .thenReturn(Arrays.asList(0, 1));

    // When:
    when(client.getCurrentOffsets(storeId, lssId, 0))
        .thenReturn(new CurrentOffsets(Optional.empty(), Optional.empty()));
    when(client.getCurrentOffsets(storeId, lssId, 1))
        .thenReturn(new CurrentOffsets(Optional.empty(), Optional.empty()));

    // Then:
    final var clientUtil = new RS3ClientUtil(storeId, client, partitioner);
    final var lssMetadata = clientUtil.fetchLssMetadata(lssId);
    assertThat(
        lssMetadata.lastWrittenOffset(),
        is(ResponsiveStoreRegistration.NO_COMMITTED_OFFSET)
    );
  }

}