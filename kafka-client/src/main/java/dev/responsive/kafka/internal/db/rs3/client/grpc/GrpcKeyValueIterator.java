/*
 * Copyright 2025 Responsive Computing, Inc.
 *
 * This source code is licensed under the Responsive Business Source License Agreement v1.0
 * available at:
 *
 * https://www.responsive.dev/legal/responsive-bsl-10
 *
 * This software requires a valid Commercial License Key for production use. Trial and commercial
 * licenses can be obtained at https://www.responsive.dev
 */

package dev.responsive.kafka.internal.db.rs3.client.grpc;

import com.google.protobuf.ByteString;
import dev.responsive.kafka.internal.db.rs3.client.RS3Exception;
import dev.responsive.kafka.internal.db.rs3.client.RS3TransientException;
import dev.responsive.kafka.internal.db.rs3.client.RangeBound;
import dev.responsive.rs3.Rs3;
import io.grpc.stub.StreamObserver;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Internal iterator implementation which supports retries using RS3's asynchronous
 * Range API.
 */
public class GrpcKeyValueIterator implements KeyValueIterator<Bytes, byte[]> {
  private static final Logger LOG = LoggerFactory.getLogger(GrpcKeyValueIterator.class);

  private final GrpcRangeRequestProxy requestProxy;
  private final GrpcMessageQueue<Message> queue;
  private RangeBound startBound;
  private RangeResultObserver resultObserver;

  public GrpcKeyValueIterator(
      RangeBound initialStartBound,
      GrpcRangeRequestProxy requestProxy
  ) {
    this.requestProxy = requestProxy;
    this.queue = new GrpcMessageQueue<>();
    this.startBound = initialStartBound;
    sendRangeRequest();
  }

  private void sendRangeRequest() {
    // Note that backoff on retry is handled internally by the request proxy
    resultObserver = new RangeResultObserver();
    requestProxy.send(startBound, resultObserver);
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
    if (resultObserver != null) {
      resultObserver.cancel();
    }
  }

  Optional<KeyValue<Bytes, byte[]>> peekNextKeyValue() {
    while (true) {
      try {
        final var message = queue.peek();
        return tryUnwrapKeyValue(message);
      } catch (RS3TransientException e) {
        queue.poll();
        sendRangeRequest();
      } catch (RuntimeException e) {
        // Leave unexpected errors in the queue so that they will continue
        // to be propagated.
        throw e;
      }
    }
  }

  private Optional<KeyValue<Bytes, byte[]>> tryUnwrapKeyValue(final Message message) {
    return message.map(new Mapper<>() {
      @Override
      public Optional<KeyValue<Bytes, byte[]>> map(final EndOfStream endOfStream) {
        return Optional.empty();
      }

      @Override
      public Optional<KeyValue<Bytes, byte[]>> map(final StreamError error) {
        throw GrpcRs3Util.wrapThrowable(error.exception);
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

  private class RangeResultObserver implements StreamObserver<Rs3.RangeResult> {
    private final AtomicReference<Throwable> error = new AtomicReference<>();

    @Override
    public void onNext(final Rs3.RangeResult rangeResult) {
      if (error.get() != null) {
        LOG.debug("Failed to send range result since the observer has already failed");
      } else if (rangeResult.getType() == Rs3.RangeResult.Type.END_OF_STREAM) {
        queue.put(new EndOfStream());
      } else {
        final var result = rangeResult.getResult();
        queue.put(new Result(result.getKey(), result.getValue()));
      }
    }

    @Override
    public void onError(final Throwable throwable) {
      if (this.error.compareAndSet(null, throwable)) {
        queue.put(new StreamError(throwable));
      } else {
        LOG.debug("Failed to record error since the observer has already failed", throwable);
      }
    }

    @Override
    public void onCompleted() {
      // We treat this as a transient error because we are looking for the explicit
      // END_OF_STREAM result that is sent by the server.
      onError(new StreamCompletedException("onCompleted fired"));
    }

    public void cancel() {
      this.error.compareAndSet(null, new RS3Exception("Range result observer cancelled"));
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

  private static class StreamCompletedException extends RS3TransientException {
    private static final long serialVersionUID = 0L;

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
