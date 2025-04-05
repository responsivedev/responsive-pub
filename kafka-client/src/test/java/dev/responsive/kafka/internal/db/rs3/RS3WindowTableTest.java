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
import dev.responsive.kafka.internal.db.rs3.client.StreamSender;
import dev.responsive.kafka.internal.db.rs3.client.StreamSenderMessageReceiver;
import dev.responsive.kafka.internal.db.rs3.client.WalEntry;
import dev.responsive.kafka.internal.utils.WindowedKey;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
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

  @Mock
  private StreamSenderMessageReceiver<WalEntry, Optional<Long>> sendRecv;

  @Mock
  private StreamSender<WalEntry> streamSender;

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

  @Test
  public void shouldWriteWindowedKeyValues() throws Exception {
    // Given:
    final var storeId = UUID.randomUUID();
    final var kafkaPartition = 5;
    final var lssId = new LssId(kafkaPartition);
    final var table = new RS3WindowTable("table", storeId, client, partitioner);
    final var pssId = 0;
    final var consumedOffset = 4L;

    // When:
    final var sendRecvCompletion = new CompletableFuture<Optional<Long>>();
    when(sendRecv.completion()).thenReturn(sendRecvCompletion);
    when(sendRecv.sender()).thenReturn(streamSender);
    when(sendRecv.isActive()).thenAnswer(invocation -> !sendRecvCompletion.isDone());
    when(partitioner.pssForLss(lssId)).thenReturn(singletonList(pssId));
    when(client.getCurrentOffsets(storeId, lssId, pssId))
        .thenReturn(new CurrentOffsets(Optional.empty(), Optional.empty()));
    when(client.writeWalSegmentAsync(storeId, lssId, pssId, Optional.empty(), consumedOffset))
        .thenReturn(sendRecv);

    // Then:
    final var flushManager = table.init(kafkaPartition);
    final var writer = flushManager.createWriter(pssId, consumedOffset);
    writer.insert(
        new WindowedKey(utf8Bytes("super"), 0L),
        utf8Bytes("mario"),
        5L
    );
    writer.insert(
        new WindowedKey(utf8Bytes("super"), 10L),
        utf8Bytes("mario"),
        15L
    );
    sendRecvCompletion.complete(Optional.of(consumedOffset));

    final var completion = writer.flush();
    assertThat(completion.toCompletableFuture().isDone(), is(true));
    final var result = completion.toCompletableFuture().get();
    assertThat(result.wasApplied(), is(true));
    assertThat(result.tablePartition(), is(pssId));
  }

  private static byte[] utf8Bytes(String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }

}