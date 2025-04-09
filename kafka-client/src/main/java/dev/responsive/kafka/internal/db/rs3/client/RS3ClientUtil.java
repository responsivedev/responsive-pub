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

import dev.responsive.kafka.internal.db.rs3.PssPartitioner;
import dev.responsive.kafka.internal.stores.ResponsiveStoreRegistration;
import java.util.HashMap;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RS3ClientUtil {
  private static final Logger LOG = LoggerFactory.getLogger(RS3ClientUtil.class);

  private final UUID storeId;
  private final PssPartitioner partitioner;
  private final RS3Client client;

  public RS3ClientUtil(
      final UUID storeId,
      final RS3Client client,
      final PssPartitioner partitioner
  ) {
    this.storeId = storeId;
    this.partitioner = partitioner;
    this.client = client;
  }

  public LssMetadata fetchLssMetadata(LssId lssId) {
    // TODO: we should write an empty segment periodically to any PSS that we haven't
    //       written to to bump the written offset
    final HashMap<Integer, Optional<Long>> writtenOffsets = new HashMap<>();
    var lastWrittenOffset = ResponsiveStoreRegistration.NO_COMMITTED_OFFSET;
    for (final int pss : partitioner.pssForLss(lssId)) {
      final var offsets = client.getCurrentOffsets(storeId, lssId, pss);
      final var writtenOffset = offsets.writtenOffset();

      if (writtenOffset.isPresent()) {
        final var offset = writtenOffset.get();
        if (offset > lastWrittenOffset) {
          lastWrittenOffset = writtenOffset.get();
        }
      }

      writtenOffsets.put(pss, offsets.writtenOffset());
    }

    final var writtenOffsetsStr = writtenOffsets.entrySet().stream()
        .map(e -> String.format("%s -> %s",
                                e.getKey(),
                                e.getValue().map(Object::toString).orElse("none")))
        .collect(Collectors.joining(","));
    LOG.info("Restore RS3 table from offset {} for {}. recorded written offsets: {}",
             lastWrittenOffset,
             lssId,
             writtenOffsetsStr
    );

    return new LssMetadata(lastWrittenOffset, writtenOffsets);
  }


}
