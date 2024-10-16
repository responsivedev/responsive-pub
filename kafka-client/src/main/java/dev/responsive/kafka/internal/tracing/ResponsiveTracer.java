package dev.responsive.kafka.internal.tracing;

import com.google.errorprone.annotations.MustBeClosed;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;

public interface ResponsiveTracer {
  String TAG_PARTITION = "partition";
  String TAG_TOPIC = "topic";
  String TAG_TASK = "task";
  String TAG_THREAD = "thread";
  String TAG_STORE = "store";
  String TAG_CLIENT_ID = "client-id";

  enum Level {
    DEBUG,
    INFO,
    ERROR
  }

  /**
   * Set a key/value in the context for this builder
   */
  ResponsiveTracer withTag(String key, String value);

  ResponsiveTracer withPartition(TopicPartition partition);

  ResponsiveTracer withTask(String topic);

  ResponsiveTracer withThread(String threadId);

  ResponsiveTracer withStore(String storeName);

  ResponsiveTracer withClientId(String clientId);

  ResponsiveTracer clone(final Logger logger);

  ResponsiveSpan span(String label, Level level);

  /**
   * Emit a point-in-time span at info level
   */
  void info(String label, String message);

  /**
   * Emit a point-in-time span at debug level
   */
  void debug(String label, String message);

  /**
   * Emit a point-in-time span at error level
   */
  void error(String label, String message);

  /**
   * Emit a point-in-time span at error level
   */
  void error(String label, String message, Throwable e);

  interface Scope extends AutoCloseable {
    @MustBeClosed
    @Override
    public void close();
  }
}
