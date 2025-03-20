package dev.responsive.kafka.internal.db.rs3.client.grpc;

import dev.responsive.rs3.RS3Grpc;
import dev.responsive.rs3.Rs3;
import io.grpc.stub.StreamObserver;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;

public class GrpcKeyValueIterator implements KeyValueIterator<Bytes, byte[]> {
  private final RS3Grpc.RS3Stub stub;
  private final Rs3.RangeRequest.Builder requestBuilder;
  private final BlockingQueue<Rs3.RangeResult> queue;
  private final AtomicBoolean done = new AtomicBoolean(false);

  private Optional<KeyValue<Bytes, byte[]>> lastKey;

  @Override
  public boolean hasNext() {

    return false;
  }

  @Override
  public KeyValue<Bytes, byte[]> next() {
    if (done.get()) {
      throw new NoSuchElementException();
    } else {
      final var result = queue.take();
      if (result.getType() == Rs3.RangeResult.Type.END_OF_STREAM) {
        done.set(true);
        throw new NoSuchElementException();
      } else {
        final var keyValue = result.getResult();
        return new KeyValue<>(
            Bytes.wrap(keyValue.getKey().toByteArray()),
            keyValue.getValue().toByteArray()
        );
      }
    }
  }

  @Override
  public void close() {

  }

  @Override
  public Bytes peekNextKey() {
    return null;
  }

  private class ScanObserver implements StreamObserver<Rs3.RangeResult> {
    @Override
    public void onNext(final Rs3.RangeResult rangeResult) {
      queue.put(rangeResult);
    }

    @Override
    public void onError(final Throwable throwable) {

    }

    @Override
    public void onCompleted() {

    }
  }

}
