package dev.responsive.kafka.internal.snapshot.topic;

class SynchronizedConsumerPosition {
  private long position = 0;

  synchronized void waitTillConsumerPosition(long targetPosition) {
    while (position < targetPosition) {
      try {
        wait();
      } catch (final InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  synchronized void updateConsumerPosition(long position) {
    this.position = position;
    notifyAll();
  }
}
