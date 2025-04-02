package dev.responsive.kafka.internal.db.rs3.client.grpc;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.concurrent.Executors;
import org.junit.jupiter.api.Test;

class GrpcMessageQueueTest {

  @Test
  public void shouldPropagateAllValuesInOrder() throws Exception {
    final var queue = new GrpcMessageQueue<Integer>();
    final var executor = Executors.newFixedThreadPool(2);
    final var producer = executor.submit(() -> {
      for (int i = 0; i < 100; i++) {
        queue.put(i);
      }
      return true;
    });
    final var consumer = executor.submit(() -> {
      for (int i = 0; i < 100; i++) {
        assertThat(queue.peek(), is(i));
        assertThat(queue.poll(), is(i));
      }
      return true;
    });
    executor.shutdown();
    assertThat(producer.get(), is(true));
    assertThat(consumer.get(), is(true));
  }

  @Test
  public void shouldBlockPollWhileWaitingValue() throws Exception {
    final var queue = new GrpcMessageQueue<Integer>();
    final var executor = Executors.newFixedThreadPool(1);
    final var consumer = executor.submit(queue::poll);
    assertThat(consumer.isDone(), is(false));
    queue.put(5);
    assertThat(consumer.get(), is(5));
  }

}