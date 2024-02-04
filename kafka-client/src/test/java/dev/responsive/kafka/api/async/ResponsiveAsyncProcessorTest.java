package dev.responsive.kafka.api.async;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import dev.responsive.kafka.api.async.AsyncProcessor.Finalizer;
import dev.responsive.kafka.api.async.ResponsiveAsyncProcessor.UnflushedRecords;
import dev.responsive.kafka.internal.stores.ResponsiveKeyValueStore;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ResponsiveAsyncProcessorTest {
  private static final TaskId TASK_ID = new TaskId(10, 11);

  @Mock
  private ResponsiveKeyValueStore kvStore;
  @Mock
  private ProcessorContext<Integer, String> context;
  @Mock
  private Finalizer<Integer, String> finalizer;
  private final TestStateStore internalStore = new TestStateStore();
  int offset = 1;
  private RecordMetadata metadata;
  private TestAsyncProcessor wrapped = new TestAsyncProcessor();

  private ResponsiveAsyncProcessor<String, Integer, Integer, String> processor;

  @BeforeEach
  public void setup() {
    when(context.getStateStore("store")).thenReturn(kvStore);
    when(context.getStateStore("async-processor")).thenReturn(internalStore);
    when(context.taskId()).thenReturn(TASK_ID);
    when(context.recordMetadata()).thenAnswer(a -> Optional.of(metadata));
    processor = new ResponsiveAsyncProcessor<>(wrapped, 4);
    processor.init(context);
  }

  @Test
  public void shouldBlockWhenBufferFull() throws InterruptedException {
    final Thread t = new Thread(() -> {
      process(new Record<>("foo", 1, 100));
      process(new Record<>("bar", 2, 101));
      process(new Record<>("baz", 3, 102));
      process(new Record<>("boz", 4, 103));
    });
    t.start();
    // good enough for development :)
    t.join(3000);
    assertThat(t.isAlive(), is(true));
  }

  @Test
  public void shouldStoreBuffer() {
    // given:
    process(new Record<>("foo", 1, 100));
    process(new Record<>("bar", 2, 101));
    process(new Record<>("baz", 3, 102));

    // when:
    final UnflushedRecords<String, Integer> unflushed = internalStore.get(TASK_ID + ".unflushed");

    // then:
    assertThat(unflushed.records().size(), is(3));
  }

  @Test
  public void shouldCallFinalizersForCompletedRecords() {
    // given:
    process(new Record<>("foo", 1, 100));
    wrapped.futures.forEach((k, v) -> v.complete(finalizer));

    // when:
    process(new Record<>("bar", 2, 101));

    // then:
    verify(finalizer).maybeForward(any(), any());
  }

  @Test
  public void shouldBlockProcessingOfKeyIfAlreadyProcessing() {
    // given:
    process(new Record<>("foo", 1, 100));
    final CompletableFuture<?> future = wrapped.futures.get("foo");

    // when:
    process(new Record<>("foo", 2, 101));

    // then:
    assertThat(wrapped.futures.get("foo"), is(future));
  }

  private void process(final Record<String, Integer> record) {
    offset += 1;
    metadata = new ProcessorRecordContext(
        record.timestamp(),
        offset,
        1,
        "topic",
        new RecordHeaders()
    );
    processor.process(record);
  }

  private static class TestAsyncProcessor
      implements AsyncProcessor<String, Integer, Integer, String> {
    private Map<String, CompletableFuture<Finalizer<Integer, String>>> futures = new HashMap<>();

    @Override
    public List<String> stores() {
      return List.of("store");
    }

    @Override
    public Future<Finalizer<Integer, String>> processAsync(final Map<String, AsyncStore> stores,
                                                           final Record<String, Integer> record) {
      return futures.compute(record.key(), (k, ev) -> new CompletableFuture<>());
    }

    @Override
    public void close() {

    }
  }

  private static class TestStateStore
      implements KeyValueStore<String, UnflushedRecords<String, Integer>> {
    private final Map<String, UnflushedRecords<String, Integer>> data = new HashMap<>();

    @Override
    public void put(final String key, final UnflushedRecords<String, Integer> value) {
      data.put(key, value);
    }

    @Override
    public UnflushedRecords<String, Integer> putIfAbsent(
        final String key,
        final UnflushedRecords<String, Integer> value
    ) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void putAll(final List<KeyValue<String, UnflushedRecords<String, Integer>>> entries) {
      throw new UnsupportedOperationException();
    }

    @Override
    public UnflushedRecords<String, Integer> delete(final String key) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String name() {
      return "async-processor";
    }

    @Override
    @SuppressWarnings("deprecation")
    public void init(final org.apache.kafka.streams.processor.ProcessorContext context,
                     final StateStore root) {

    }

    @Override
    public void flush() {

    }

    @Override
    public void close() {

    }

    @Override
    public boolean persistent() {
      return false;
    }

    @Override
    public boolean isOpen() {
      return false;
    }

    @Override
    public UnflushedRecords<String, Integer> get(final String key) {
      return data.get(key);
    }

    @Override
    public KeyValueIterator<String, UnflushedRecords<String, Integer>> range(final String from, final String to) {
      throw new UnsupportedOperationException();
    }

    @Override
    public KeyValueIterator<String, UnflushedRecords<String, Integer>> all() {
      throw new UnsupportedOperationException();
    }

    @Override
    public long approximateNumEntries() {
      return data.size();
    }
  }
}