package dev.responsive.kafka.internal.db.rs3.client;

public interface StreamSender<S> {
  void sendNext(S msg);

  void finish();

  void cancel();
}
