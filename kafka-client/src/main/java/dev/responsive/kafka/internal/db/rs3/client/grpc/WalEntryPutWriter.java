package dev.responsive.kafka.internal.db.rs3.client.grpc;

import com.google.protobuf.ByteString;
import dev.responsive.kafka.internal.db.rs3.client.Delete;
import dev.responsive.kafka.internal.db.rs3.client.Put;
import dev.responsive.kafka.internal.db.rs3.client.WalEntry;
import dev.responsive.kafka.internal.db.rs3.client.WindowedDelete;
import dev.responsive.kafka.internal.db.rs3.client.WindowedPut;
import dev.responsive.rs3.Rs3;

public class WalEntryPutWriter implements WalEntry.Visitor {
  private final Rs3.WriteWALSegmentRequest.Put.Builder builder;

  public WalEntryPutWriter(final Rs3.WriteWALSegmentRequest.Put.Builder builder) {
    this.builder = builder;
  }

  @Override
  public void visit(final Put put) {
    builder.setKey(ByteString.copyFrom(put.key()));
    builder.setValue(ByteString.copyFrom(put.value()));
    builder.setTtl(Rs3.Ttl.newBuilder().setTtlType(Rs3.Ttl.TtlType.DEFAULT).build());
  }

  @Override
  public void visit(final Delete delete) {
    builder.setKey(ByteString.copyFrom(delete.key()));
    builder.setTtl(Rs3.Ttl.newBuilder().setTtlType(Rs3.Ttl.TtlType.DEFAULT).build());
  }

  @Override
  public void visit(final WindowedDelete windowedDelete) {
    visit((Delete) windowedDelete);
    builder.setWindowTimestamp(windowedDelete.windowTimestamp());
  }

  @Override
  public void visit(final WindowedPut windowedPut) {
    visit((Put) windowedPut);
    builder.setWindowTimestamp(windowedPut.windowTimestamp());
  }
}
