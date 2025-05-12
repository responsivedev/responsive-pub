package dev.responsive.kafka.internal.db.rs3.client.grpc;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import dev.responsive.kafka.internal.db.rs3.client.Delete;
import dev.responsive.kafka.internal.db.rs3.client.Put;
import dev.responsive.kafka.internal.db.rs3.client.WindowedDelete;
import dev.responsive.kafka.internal.db.rs3.client.WindowedPut;
import dev.responsive.rs3.Rs3;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class WalEntryPutWriterTest {

  @Test
  public void shouldCreatePutRequest() {
    final var builder = Rs3.WriteWALSegmentRequest.newBuilder();
    final var writer = new WalEntryPutWriter(builder);

    final var key = "foo";
    final var value = "bar";
    writer.visit(new Put(
        key.getBytes(StandardCharsets.UTF_8),
        value.getBytes(StandardCharsets.UTF_8)
    ));

    final var request = builder.build();
    assertThat(request.hasPut(), is(true));

    final var kv = request.getPut().getKv();
    assertThat(kv.hasBasicKv(), is(true));

    final var windowKv = kv.getBasicKv();
    assertThat(windowKv.getKey().getKey().toStringUtf8(), is(key));
    assertThat(windowKv.getValue().getValue().toStringUtf8(), is(value));
  }

  @Test
  public void shouldCreateWindowedPutRequest() {
    final var builder = Rs3.WriteWALSegmentRequest.newBuilder();
    final var writer = new WalEntryPutWriter(builder);

    final var key = "foo";
    final var value = "bar";
    final var timestamp = 15L;
    final var windowStartTimeMs = 10L;
    writer.visit(new WindowedPut(
        key.getBytes(StandardCharsets.UTF_8),
        value.getBytes(StandardCharsets.UTF_8),
        timestamp,
        windowStartTimeMs
    ));

    final var request = builder.build();
    assertThat(request.hasPut(), is(true));

    final var kv = request.getPut().getKv();
    assertThat(kv.hasWindowKv(), is(true));

    final var windowKv = kv.getWindowKv();
    assertThat(windowKv.getKey().getKey().toStringUtf8(), is(key));
    assertThat(windowKv.getKey().getWindowTimestamp(), is(windowStartTimeMs));
    assertThat(windowKv.getValue().getValue().toStringUtf8(), is(value));
    assertThat(windowKv.getValue().getEventTimestamp(), is(timestamp));
  }

  @Test
  public void shouldCreateDeleteRequest() {
    final var builder = Rs3.WriteWALSegmentRequest.newBuilder();
    final var writer = new WalEntryPutWriter(builder);

    final var key = "foo";

    writer.visit(new Delete(key.getBytes(StandardCharsets.UTF_8)));

    final var request = builder.build();
    assertThat(request.hasDelete(), is(true));

    final var builtKey = request.getDelete().getKey();
    assertThat(builtKey.hasBasicKey(), is(true));

    final var windowKey = builtKey.getBasicKey();
    assertThat(windowKey.getKey().toStringUtf8(), is(key));
  }

  @Test
  public void shouldCreateWindowedDeleteRequest() {
    final var builder = Rs3.WriteWALSegmentRequest.newBuilder();
    final var writer = new WalEntryPutWriter(builder);

    final var key = "foo";
    final var windowStartTimeMs = 10L;

    writer.visit(new WindowedDelete(
        key.getBytes(StandardCharsets.UTF_8),
        windowStartTimeMs
    ));

    final var request = builder.build();
    assertThat(request.hasDelete(), is(true));

    final var builtKey = request.getDelete().getKey();
    assertThat(builtKey.hasWindowKey(), is(true));

    final var windowKey = builtKey.getWindowKey();
    assertThat(windowKey.getKey().toStringUtf8(), is(key));
    assertThat(windowKey.getWindowTimestamp(), is(windowStartTimeMs));
  }

}