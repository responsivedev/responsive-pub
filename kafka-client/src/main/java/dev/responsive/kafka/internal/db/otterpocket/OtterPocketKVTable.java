package dev.responsive.kafka.internal.db.otterpocket;

import dev.responsive.kafka.internal.db.RemoteKVTable;
import dev.responsive.kafka.internal.db.WriterFactory;
import java.util.Objects;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueIterator;

public class OtterPocketKVTable implements RemoteKVTable<Object> {
  private final KVOtterPocketClient client;

  public OtterPocketKVTable(final KVOtterPocketClient client) {
    this.client = Objects.requireNonNull(client);
  }

  @Override
  public byte[] get(final int kafkaPartition, final Bytes key, final long minValidTs) {
    return client.get(kafkaPartition, key.get());
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> range(final int kafkaPartition, final Bytes from,
                                               final Bytes to,
                                               final long minValidTs) {
    throw new UnsupportedOperationException();
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> all(final int kafkaPartition, final long minValidTs) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long approximateNumEntries(final int kafkaPartition) {
    return 0;
  }

  @Override
  public String name() {
    return "otterpocket-poc";
  }

  @Override
  public WriterFactory<Bytes, ?> init(final int kafkaPartition) {
    return new OtterPocketWriterFactory("otterpocket", client, kafkaPartition);
  }

  @Override
  public Object insert(final int kafkaPartition, final Bytes key, final byte[] value,
                       final long epochMillis) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object delete(final int kafkaPartition, final Bytes key) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long fetchOffset(final int kafkaPartition) {
    return 0;
  }

  @Override
  public Object setOffset(final int kafkaPartition, final long offset) {
    return null;
  }
}
