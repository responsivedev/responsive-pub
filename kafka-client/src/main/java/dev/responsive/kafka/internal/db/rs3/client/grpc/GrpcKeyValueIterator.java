package dev.responsive.kafka.internal.db.rs3.client.grpc;

import com.google.protobuf.ByteString;
import dev.responsive.kafka.internal.db.rs3.client.RS3Exception;
import dev.responsive.kafka.internal.db.rs3.client.RangeBound;
import dev.responsive.rs3.RS3Grpc;
import dev.responsive.rs3.Rs3;
import io.grpc.stub.StreamObserver;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;

public class GrpcKeyValueIterator implements KeyValueIterator<Bytes, byte[]> {
  private final RS3Grpc.RS3Stub stub;
  private final Rs3.RangeRequest.Builder requestBuilder;
  private final BlockingQueue<Message> queue;

  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final AtomicBoolean done = new AtomicBoolean(false);

  private final RangeBound endBound;
  private RangeBound startBound;


  @Override
  public boolean hasNext() {

    return false;
  }

  public void sendRequest() {
    final var request = requestBuilder.build();

    stub.range(request, new ScanObserver());
  }

  @Override
  public KeyValue<Bytes, byte[]> next() {
    if (closed.get()) {
      throw new IllegalStateException();
    } else if (done.get()) {
      throw new NoSuchElementException();
    } else {
      try {
        final var result = queue.take();
        return result.map(new Mapper<>() {
          @Override
          public KeyValue<Bytes, byte[]> map(final EndOfStream endOfStream) {
            done.set(true);
            throw new NoSuchElementException();
          }

          @Override
          public KeyValue<Bytes, byte[]> map(final StreamError error) {
            return null;
          }

          @Override
          public KeyValue<Bytes, byte[]> map(final Result result) {
            final var key = Bytes.wrap(result.key.toByteArray());
            final var value = result.value.toByteArray();
            return new KeyValue<>(key, value);
          }
        });
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RS3Exception(e);
      }
    }
  }

  @Override
  public void close() {
    this.done.set(true);
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

  private interface Message {
    <T> T map(Mapper mapper);
  }

  private static class EndOfStream implements Message {
    @Override
    public <T> T map(final Mapper mapper) {
      return mapper.map(this);
    }
  }

  private static class Result implements Message {
    private final ByteString key;
    private final ByteString value;

    @Override
    public <T> T map(final Mapper mapper) {
      return mapper.map(this);
    }
  }

  private static class StreamError implements Message {
    private final Throwable error;
    StreamError(final Throwable error) {
      this.error = error;
    }

    @Override
    public <T> T map(final Mapper mapper) {
      return mapper.map(this);
    }
  }

  private interface Mapper<T> {
    T map(EndOfStream endOfStream);
    T map(StreamError error);
    T map(Result result);
  }

}
