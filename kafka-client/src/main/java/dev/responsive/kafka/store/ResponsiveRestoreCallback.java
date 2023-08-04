package dev.responsive.kafka.store;

import java.util.Collection;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.internals.RecordBatchingStateRestoreCallback;

public class ResponsiveRestoreCallback implements RecordBatchingStateRestoreCallback {

  private final Consumer<Collection<ConsumerRecord<byte[], byte[]>>> batchRestorer;
  private final Supplier<Boolean> allowRestore;

  public ResponsiveRestoreCallback(
      final Consumer<Collection<ConsumerRecord<byte[], byte[]>>> batchRestorer,
      final Supplier<Boolean> allowRestore
  ) {
    this.batchRestorer = batchRestorer;
    this.allowRestore = allowRestore;
  }

  @Override
  public void restoreBatch(final Collection<ConsumerRecord<byte[], byte[]>> records) {
    if (allowRestore.get()) {
      batchRestorer.accept(records);
    }
  }
}
