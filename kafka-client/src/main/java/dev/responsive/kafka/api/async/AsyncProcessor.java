package dev.responsive.kafka.api.async;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

public interface AsyncProcessor<KIn, VIn, KOut, VOut> {
  List<String> stores();

  // must be read-only (can't write to stores or forward)
  Future<Finalizer<KOut, VOut>> processAsync(
      Map<String, AsyncKeyValueStore<?, ?>> stores, Record<KIn, VIn> record);

  void close();

  interface Finalizer<KOut, VOut> {
    void maybeForward(Map<String, StateStore> stores, ProcessorContext<KOut, VOut> context);
  }
}