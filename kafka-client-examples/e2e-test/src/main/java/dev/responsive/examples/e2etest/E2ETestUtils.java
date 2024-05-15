package dev.responsive.examples.e2etest;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class E2ETestUtils {
  private static final Logger LOG = LoggerFactory.getLogger(E2ETestUtils.class);

  static void maybeCreateTopics(
      final Map<String, Object> properties,
      final int partitions,
      final List<String> topics
  ) {
    try (final Admin admin = Admin.create(properties)) {
      for (final var topic : topics) {
        LOG.info("create topic {}", topic);
        try {
          admin.createTopics(List.of(new NewTopic(topic, partitions, (short) 1)))
              .all().get();
        } catch (final ExecutionException | InterruptedException e) {
          if (e.getCause() instanceof TopicExistsException) {
            continue;
          }
          throw new RuntimeException(e);
        } catch (final RuntimeException e) {
          LOG.info("Error creating topic: " + e);
        }
      }
    }
  }

  static void retryFor(final Runnable task, Duration timeout) {
    final Instant start = Instant.now();
    RuntimeException last = new IllegalStateException();
    while (Instant.now().isBefore(start.plus(timeout))) {
      try {
        task.run();
        return;
      } catch (final RuntimeException e) {
        last = e;
        LOG.error("task failed. retry in 3 seconds", e);
      }
      try {
        Thread.sleep(3000);
      } catch (final InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    throw last;
  }

  private E2ETestUtils() {
  }
}
