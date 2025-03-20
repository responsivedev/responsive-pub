package dev.responsive.kafka.internal.db.rs3.client.grpc;

import com.google.protobuf.ByteString;
import dev.responsive.kafka.internal.db.rs3.client.RS3Exception;
import dev.responsive.kafka.internal.db.rs3.client.RangeBound;
import dev.responsive.rs3.Rs3;
import io.grpc.stub.StreamObserver;
import java.util.NoSuchElementException;
import java.util.Optional;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;

public class GrpcKeyValueIterator implements KeyValueIterator<Bytes, byte[]> {
  private final GrpcRangeRequestProxy requestProxy;
  private final GrpcMessageQueue<Message> queue;
  private RangeBound startBound;

  public GrpcKeyValueIterator(GrpcRangeRequestProxy requestProxy) {
    this.requestProxy = requestProxy;
    this.queue = new GrpcMessageQueue<>();
    requestProxy.send(startBound, new ScanObserver());
  }

  @Override
  public boolean hasNext() {
    return peekNextKeyValue().isPresent();
  }

  @Override
  public KeyValue<Bytes, byte[]> next() {
    final var nextKeyValue = peekNextKeyValue();
    if (nextKeyValue.isPresent()) {
      queue.poll();
      final var keyValue = nextKeyValue.get();
      this.startBound = RangeBound.exclusive(keyValue.key.get());
      return keyValue;
    } else {
      throw new NoSuchElementException();
    }
  }

  @Override
  public void close() {
    // TODO:
  }

  Optional<KeyValue<Bytes, byte[]>> peekNextKeyValue() {
    final var message = queue.peek();
    return message.map(new Mapper<>() {
      @Override
      public Optional<KeyValue<Bytes, byte[]>> map(final EndOfStream endOfStream) {
        return Optional.empty();
      }

      @Override
      public Optional<KeyValue<Bytes, byte[]>> map(final StreamError error) {
        // TODO: Retry on transient failures
        throw new RS3Exception(error.exception);
      }

      @Override
      public Optional<KeyValue<Bytes, byte[]>> map(final Result result) {
        final var key = Bytes.wrap(result.key.toByteArray());
        final var value = result.value.toByteArray();
        return Optional.of(new KeyValue<>(key, value));
      }
    });
  }

  @Override
  public Bytes peekNextKey() {
    return peekNextKeyValue()
        .map(bytesKeyValue -> bytesKeyValue.key)
        .orElse(null);
  }

  private class ScanObserver implements StreamObserver<Rs3.RangeResult> {
    @Override
    public void onNext(final Rs3.RangeResult rangeResult) {
      if (rangeResult.getType() == Rs3.RangeResult.Type.END_OF_STREAM) {
        queue.put(new EndOfStream());
      } else {
        final var result = rangeResult.getResult();
        queue.put(new Result(result.getKey(), result.getValue()));
      }
    }

    @Override
    public void onError(final Throwable throwable) {
      queue.put(new StreamError(throwable));
    }

    @Override
    public void onCompleted() {
      queue.put(new StreamError(new StreamCompletedException("onCompleted fired")));
    }
  }

  private interface Message {
    <T> T map(Mapper<T> mapper);
  }

  private static class EndOfStream implements Message {
    @Override
    public <T> T map(final Mapper<T> mapper) {
      return mapper.map(this);
    }
  }

  private static class Result implements Message {
    private final ByteString key;
    private final ByteString value;

    private Result(final ByteString key, final ByteString value) {
      this.key = key;
      this.value = value;
    }

    @Override
    public <T> T map(final Mapper<T> mapper) {
      return mapper.map(this);
    }
  }

  private static class StreamError implements Message {
    private final Throwable exception;
    StreamError(final Throwable error) {
      this.exception = error;
    }

    @Override
    public <T> T map(final Mapper<T> mapper) {
      return mapper.map(this);
    }
  }

  private static class StreamCompletedException extends RS3Exception {
    public StreamCompletedException(final String message) {
      super(message);
    }
  }

  private interface Mapper<T> {
    T map(EndOfStream endOfStream);
    T map(StreamError error);
    T map(Result result);
  }

}
