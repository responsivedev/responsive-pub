package dev.responsive.kafka.internal.db.rs3.client.grpc;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.concurrent.Executors;
import org.junit.jupiter.api.Test;

class GrpcMessageQueueTest {

  @Test
  public void test() {
    final var queue = new GrpcMessageQueue<Integer>();
    final var executor = Executors.newFixedThreadPool(2);
    executor.submit(() -> {
      for (int i = 0; i < 100; i++) {
        queue.put(i);
      }
    });
    executor.submit(() -> {
      for (int i = 0; i < 100; i++) {
        assertThat(queue.poll(), is(i));
      }
    });

    executor.shutdown();
  }

}