/*
 * Copyright 2024 Responsive Computing, Inc.
 *
 * This source code is licensed under the Responsive Business Source License Agreement v1.0
 * available at:
 *
 * https://www.responsive.dev/legal/responsive-bsl-10
 *
 * This software requires a valid Commercial License Key for production use. Trial and commercial
 * licenses can be obtained at https://www.responsive.dev
 */

package dev.responsive.kafka.internal.stores;

import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.function.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

public final class ResponsiveStoreRegistration {
  public static final long NO_COMMITTED_OFFSET = -1L; // buffer is initialized but no prior offset

  private final Logger log;
  private final String storeName;
  private final TopicPartition changelogTopicPartition;
  private final Consumer<Long> onCommit;
  private final String threadId;
  private final InjectedStoreArgs injectedStoreArgs = new InjectedStoreArgs();

  private final OptionalLong startOffset; // stored offset during init, (where restore should start)

  @VisibleForTesting
  public ResponsiveStoreRegistration(
      final String storeName,
      final TopicPartition changelogTopicPartition,
      final OptionalLong startOffset,
      final Consumer<Long> onCommit,
      final String threadId
  ) {
    this.storeName = Objects.requireNonNull(storeName);
    this.changelogTopicPartition = Objects.requireNonNull(changelogTopicPartition);
    this.startOffset = startOffset;
    this.onCommit = Objects.requireNonNull(onCommit);
    this.threadId = Objects.requireNonNull(threadId);
    this.log = new LogContext(
        String.format("changelog [%s]", changelogTopicPartition)
    ).logger(ResponsiveStoreRegistration.class);
    log.debug("Created store registration with stored offset={}", startOffset);
  }

  public OptionalLong startOffset() {
    return startOffset;
  }

  public TopicPartition changelogTopicPartition() {
    return changelogTopicPartition;
  }

  public String storeName() {
    return storeName;
  }

  public Consumer<Long> onCommit() {
    return onCommit;
  }

  public InjectedStoreArgs injectedStoreArgs() {
    return injectedStoreArgs;
  }

  public String threadId() {
    return threadId;
  }
}
