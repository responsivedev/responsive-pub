package dev.responsive.kafka.internal.db.pocket.client;

public interface StreamSender<S> {
  void sendNext(S msg);

  void finish();

  void cancel();
}
