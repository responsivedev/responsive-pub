package dev.responsive.kafka.internal.db.rs3;

import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import dev.responsive.kafka.internal.db.rs3.client.CurrentOffsets;
import dev.responsive.kafka.internal.db.rs3.client.LssId;
import dev.responsive.kafka.internal.db.rs3.client.RS3Client;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.UUID;
import org.apache.kafka.common.utils.Bytes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class RS3WindowTableTest {

  @Mock
  private RS3Client client;

  @Mock
  private PssPartitioner partitioner;

  @Test
  public void shouldReturnValueFromFetchIfKeyFound() {
    // Given:
    final var storeId = UUID.randomUUID();
    final var kafkaPartition = 5;
    final var lssId = new LssId(kafkaPartition);
    final var table = new RS3WindowTable("table", storeId, client, partitioner);
    final var key = Bytes.wrap("foo".getBytes(StandardCharsets.UTF_8));
    final var timestamp = 300L;

    // When:
    final var pssId = 1;
    final byte[] value = "bar".getBytes(StandardCharsets.UTF_8);
    when(partitioner.pssForLss(lssId)).thenReturn(singletonList(pssId));
    when(partitioner.pss(any(), eq(lssId))).thenReturn(pssId);
    when(client.getCurrentOffsets(storeId, lssId, pssId))
        .thenReturn(new CurrentOffsets(Optional.of(5L), Optional.of(5L)));
    when(client.get(
        eq(storeId),
        eq(lssId),
        eq(pssId),
        eq(Optional.of(5L)),
        any())
    ).thenReturn(Optional.of(value));

    // Then:
    table.init(kafkaPartition);
    assertThat(table.fetch(kafkaPartition, key, timestamp), is(value));
  }

  @Test
  public void shouldReturnNullFromFetchIfKeyNotFound() {
    // Given:
    final var storeId = UUID.randomUUID();
    final var kafkaPartition = 5;
    final var lssId = new LssId(kafkaPartition);
    final var table = new RS3WindowTable("table", storeId, client, partitioner);
    final var key = Bytes.wrap("foo".getBytes(StandardCharsets.UTF_8));
    final var timestamp = 300L;

    // When:
    final var pssId = 1;
    when(partitioner.pssForLss(lssId)).thenReturn(singletonList(pssId));
    when(partitioner.pss(any(), eq(lssId))).thenReturn(pssId);
    when(client.getCurrentOffsets(storeId, lssId, pssId))
        .thenReturn(new CurrentOffsets(Optional.of(5L), Optional.of(5L)));
    when(client.get(
        eq(storeId),
        eq(lssId),
        eq(pssId),
        eq(Optional.of(5L)),
        any())
    ).thenReturn(Optional.empty());

    // Then:
    table.init(kafkaPartition);
    assertThat(table.fetch(kafkaPartition, key, timestamp), is(nullValue()));
  }

}