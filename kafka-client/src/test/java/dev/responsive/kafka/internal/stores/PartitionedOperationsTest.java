/*
 * Copyright 2024 Responsive Computing, Inc.
 *
 * This source code is licensed under the Responsive Business Source License Agreement v1.0
 * available at:
 *
 * https://www.responsive.dev/legal/responsive-bsl-10
 *
 * This software requires a valid Commercial License Key for production use. Trial and commercial
 * licenses can be obtained at https://www.responsive.dev
 */

package dev.responsive.kafka.internal.stores;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import dev.responsive.kafka.api.stores.ResponsiveKeyValueParams;
import dev.responsive.kafka.internal.db.RemoteKVTable;
import dev.responsive.kafka.internal.metrics.ResponsiveRestoreListener;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ExtendWith(MockitoExtension.class)
class PartitionedOperationsTest {
  private static final Logger LOG = LoggerFactory.getLogger(PartitionedOperationsTest.class);

  private static final TopicPartition TOPIC_PARTITION = new TopicPartition("topic", 3);
  private static final Instant MIGRATE_START_TTL = Instant.now().minus(Duration.ofHours(12));
  private static final Bytes KEY = Bytes.wrap("key".getBytes(StandardCharsets.UTF_8));
  private static final byte[] VALUE = "value".getBytes(StandardCharsets.UTF_8);

  @Mock
  private InternalProcessorContext<?, ?> processorContext;
  @Mock
  private RemoteKVTable<?> remoteKVTable;
  @Mock
  private CommitBuffer<Bytes, ?> commitBuffer;
  @Mock
  private ResponsiveStoreRegistry storeRegistry;
  @Mock
  private ResponsiveStoreRegistration registration;
  @Mock
  private ResponsiveRestoreListener restoreListener;

  private final ResponsiveKeyValueParams factParams = ResponsiveKeyValueParams.fact("factstr")
      .withTimeToLive(Duration.ofDays(1));

  private PartitionedOperations migrationPartitionedOperations;
  private PartitionedOperations partitionedOperations;

  @BeforeEach
  public void setup() {
    partitionedOperations = new PartitionedOperations(
        LOG,
        processorContext,
        factParams,
        remoteKVTable,
        commitBuffer,
        TOPIC_PARTITION,
        storeRegistry,
        registration,
        restoreListener,
        false,
         -1
    );
    migrationPartitionedOperations = new PartitionedOperations(
        LOG,
        processorContext,
        factParams,
        remoteKVTable,
        commitBuffer,
        TOPIC_PARTITION,
        storeRegistry,
        registration,
        restoreListener,
        true,
        MIGRATE_START_TTL.toEpochMilli()
    );
    when(registration.injectedStoreArgs()).thenReturn(new InjectedStoreArgs());
  }

  @Test
  public void shouldSkipPutForExpiredRecordOnMigrate() {
    // given:
    when(processorContext.timestamp())
        .thenReturn(MIGRATE_START_TTL.minus(Duration.ofHours(1)).toEpochMilli());

    // when:
    migrationPartitionedOperations.put(KEY, VALUE);

    // then:
    verifyNoInteractions(commitBuffer);
  }

  @Test
  public void shouldNotSkipPutForUnexpiredRecordOnMigrate() {
    // given:
    final long streamTime = MIGRATE_START_TTL.plus(Duration.ofHours(1)).toEpochMilli();
    when(processorContext.timestamp()).thenReturn(streamTime);

    // when:
    migrationPartitionedOperations.put(KEY, VALUE);

    // then:
    verify(commitBuffer).put(KEY, VALUE, streamTime);
  }

  @Test
  public void shouldNotSkipPutForExpiredRecordWhenNotMigrating() {
    // given:
    final long streamTime = MIGRATE_START_TTL.minus(Duration.ofHours(1)).toEpochMilli();
    when(processorContext.timestamp()).thenReturn(streamTime);

    // when:
    partitionedOperations.put(KEY, VALUE);

    // then:
    verify(commitBuffer).put(KEY, VALUE, streamTime);
  }
}