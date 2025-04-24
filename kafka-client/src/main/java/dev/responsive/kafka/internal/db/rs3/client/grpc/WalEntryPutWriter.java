package dev.responsive.kafka.internal.db.rs3.client.grpc;

import com.google.protobuf.ByteString;
import dev.responsive.kafka.internal.db.rs3.client.Delete;
import dev.responsive.kafka.internal.db.rs3.client.Put;
import dev.responsive.kafka.internal.db.rs3.client.WalEntry;
import dev.responsive.kafka.internal.db.rs3.client.WindowedDelete;
import dev.responsive.kafka.internal.db.rs3.client.WindowedPut;
import dev.responsive.rs3.Rs3;

public class WalEntryPutWriter implements WalEntry.Visitor {
  private final Rs3.WriteWALSegmentRequest.Builder builder;

  public WalEntryPutWriter(final Rs3.WriteWALSegmentRequest.Builder builder) {
    this.builder = builder;
  }

  @Override
  public void visit(final Put put) {
    final var kvProto = Rs3.BasicKeyValue.newBuilder()
        .setKey(Rs3.BasicKey.newBuilder().setKey(ByteString.copyFrom(put.key())))
        .setValue(Rs3.BasicValue.newBuilder().setValue(ByteString.copyFrom(put.value())));
    final var putProto = Rs3.WriteWALSegmentRequest.Put.newBuilder()
        .setKv(Rs3.KeyValue.newBuilder().setBasicKv(kvProto))
        .setTtl(Rs3.Ttl.newBuilder().setTtlType(Rs3.Ttl.TtlType.DEFAULT));
    builder.setPut(putProto);
  }

  @Override
  public void visit(final Delete delete) {
    final var keyProto = Rs3.BasicKey.newBuilder()
        .setKey(ByteString.copyFrom(delete.key()));
    final var deleteProto = Rs3.WriteWALSegmentRequest.Delete.newBuilder()
        .setKey(Rs3.Key.newBuilder().setBasicKey(keyProto));
    builder.setDelete(deleteProto);
  }

  @Override
  public void visit(final WindowedDelete windowedDelete) {
    final var keyProto = Rs3.WindowKey.newBuilder()
        .setKey(ByteString.copyFrom(windowedDelete.key()))
        .setWindowTimestamp(windowedDelete.windowTimestamp());
    final var deleteProto = Rs3.WriteWALSegmentRequest.Delete.newBuilder()
        .setKey(Rs3.Key.newBuilder().setWindowKey(keyProto));
    builder.setDelete(deleteProto);
  }

  @Override
  public void visit(final WindowedPut windowedPut) {
    final var keyProto = Rs3.WindowKey.newBuilder()
        .setKey(ByteString.copyFrom(windowedPut.key()))
        .setWindowTimestamp(windowedPut.windowTimestamp());
    final var valueProto = Rs3.WindowValue.newBuilder()
        .setValue(ByteString.copyFrom(windowedPut.value()));
    final var kvProto = Rs3.WindowKeyValue.newBuilder()
        .setKey(keyProto)
        .setValue(valueProto);
    final var putProto = Rs3.WriteWALSegmentRequest.Put.newBuilder()
        .setKv(Rs3.KeyValue.newBuilder().setWindowKv(kvProto))
        .setTtl(Rs3.Ttl.newBuilder().setTtlType(Rs3.Ttl.TtlType.DEFAULT));
    builder.setPut(putProto);
  }
}
