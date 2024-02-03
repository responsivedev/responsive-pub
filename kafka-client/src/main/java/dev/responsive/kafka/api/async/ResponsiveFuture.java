package dev.responsive.kafka.api.async;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

/* Composeable futures that compose on the thread calling get instead of the thread that
 * completes the future (like ComposeableFuture)
 */
public interface ResponsiveFuture<T> extends Future<T> {
  <U> ResponsiveFuture<U> thenApply(Function<? super T, ? extends U> applier);

  <U> ResponsiveFuture<U> thenCompose(Function<? super T, ? extends Future<U>> composer);

  static <T> ResponsiveFuture<T> of(final Future<T> wrapped) {
    return new ResponsiveComposableFuture<>(wrapped);
  }

  static <T> ResponsiveFuture<T> ofCompleted(final T result) {
    return new ResponsiveComposableFuture<>(CompletableFuture.completedFuture(result));
  }

  class ResponsiveComposedFuture<T, U> implements ResponsiveFuture<U> {
    private final Future<T> wrapped;
    private Future<U> next;
    private U result;
    private final Function<? super T, ? extends Future<U>> composer;

    public ResponsiveComposedFuture(final Future<T> wrapped,
                                    final Function<? super T, ? extends Future<U>> composer) {
      this.wrapped = wrapped;
      this.composer = composer;
    }

    @Override
    public <V> ResponsiveFuture<V> thenApply(final Function<? super U, ? extends V> applier) {
      return new ResponsiveAppliedFuture<>(this, applier);
    }

    @Override
    public <V> ResponsiveFuture<V> thenCompose(
        final Function<? super U, ? extends Future<V>> composer) {
      return new ResponsiveComposedFuture<>(this, composer);
    }

    @Override
    public boolean cancel(final boolean mayInterruptIfRunning) {
      return next == null
          ? wrapped.cancel(mayInterruptIfRunning) : next.cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled() {
       return next == null
           ? wrapped.isCancelled() : next.isCancelled();
    }

    @Override
    public boolean isDone() {
      return wrapped.isDone() && next != null && next.isDone();
    }

    @Override
    public U get() throws InterruptedException, ExecutionException {
      final T wrappedResult = wrapped.get();
      if (next == null) {
        next = composer.apply(wrappedResult);
      }
      if (result == null) {
        result = next.get();
      }
      return result;
    }

    @Override
    public U get(final long timeout, final TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
      final T wrappedResult = wrapped.get(timeout, unit);
      if (next == null) {
        next = composer.apply(wrappedResult);
      }
      if (result == null) {
        result = next.get(timeout, unit);
      }
      return result;
    }
  }

  class ResponsiveAppliedFuture<T, U> implements ResponsiveFuture<U> {
    private final Future<T> wrapped;
    private final Function<? super T, ? extends U> applier;
    private U result;

    public ResponsiveAppliedFuture(final Future<T> wrapped, final Function<? super T, ? extends U> applier) {
      this.wrapped = wrapped;
      this.applier = applier;
    }

    @Override
    public <V> ResponsiveFuture<V> thenApply(final Function<? super U, ? extends V> applier) {
      return new ResponsiveAppliedFuture<>(this, applier);
    }

    @Override
    public <V> ResponsiveFuture<V> thenCompose(
        final Function<? super U, ? extends Future<V>> composer) {
      return new ResponsiveComposedFuture<>(this, composer);
    }

    @Override
    public boolean cancel(final boolean mayInterruptIfRunning) {
      return wrapped.cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled() {
      return wrapped.isCancelled();
    }

    @Override
    public boolean isDone() {
      return wrapped.isDone();
    }

    @Override
    public U get() throws InterruptedException, ExecutionException {
      if (result == null) {
        final T wrappedResult = wrapped.get();
        result = applier.apply(wrappedResult);
      }
      return result;
    }

    @Override
    public U get(final long timeout, final TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
      if (result == null) {
        final T wrappedResult = wrapped.get(timeout, unit);
        result = applier.apply(wrappedResult);
      }
      return result;
    }
  }

  class ResponsiveComposableFuture<T> implements ResponsiveFuture<T> {
    private final Future<T> wrapped;

    public ResponsiveComposableFuture(final Future<T> wrapped) {
      this.wrapped = wrapped;
    }

    @Override
    public <U> ResponsiveFuture<U> thenApply(final Function<? super T, ? extends U> applier) {
      return new ResponsiveAppliedFuture<>(this, applier);
    }

    @Override
    public <U> ResponsiveFuture<U> thenCompose(
        final Function<? super T, ? extends Future<U>> composer) {
      return new ResponsiveComposedFuture<>(this, composer);
    }

    @Override
    public boolean cancel(final boolean mayInterruptIfRunning) {
      return wrapped.cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled() {
      return wrapped.isCancelled();
    }

    @Override
    public boolean isDone() {
      return wrapped.isDone();
    }

    @Override
    public T get() throws InterruptedException, ExecutionException {
      return wrapped.get();
    }

    @Override
    public T get(final long timeout, final TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
      return wrapped.get(timeout, unit);
    }
  }
}
