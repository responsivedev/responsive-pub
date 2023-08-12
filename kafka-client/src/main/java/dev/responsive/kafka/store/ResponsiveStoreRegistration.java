/*
 * Copyright 2023 Responsive Computing, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dev.responsive.kafka.store;

import java.util.Objects;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

public final class ResponsiveStoreRegistration {
  public static final long NO_COMMITTED_OFFSET = -1L; // buffer is initialized but no prior offset
  public static final long UNINITIALIZED_OFFSET = -2L; // buffer is not yet initialized

  private final Logger log;
  private final String storeName;
  private final TopicPartition changelogTopicPartition;
  private final Consumer<Long> onCommit;

  private final LongSupplier storedOffset; // RPC to fetch offset committed in remote table
  private final BooleanSupplier maybeInitialize; // returns true if/when initialization is done
  private long startOffset; // stored offset as of initialization, ie where restore should start

  public ResponsiveStoreRegistration(
      final String storeName,
      final TopicPartition changelogTopicPartition,
      final Consumer<Long> onCommit,
      final LongSupplier storedOffset
  ) {
    this(
        storeName,
        changelogTopicPartition,
        onCommit,
        () -> {
          throw new IllegalStateException("Already initialized stored offset for changelog "
                                              + changelogTopicPartition);
        },
        () -> {
          throw new IllegalStateException("Already initialized regular active store with changelog "
                                              + changelogTopicPartition);
        },
        storedOffset.getAsLong()
    );
  }

  public ResponsiveStoreRegistration(
      final String storeName,
      final TopicPartition changelogTopicPartition,
      final Consumer<Long> onCommit,
      final LongSupplier storedOffset,
      final BooleanSupplier maybeInitialize
  ) {
    this(
        storeName,
        changelogTopicPartition,
        onCommit,
        storedOffset,
        maybeInitialize,
        UNINITIALIZED_OFFSET
    );
  }

  private ResponsiveStoreRegistration(
      final String storeName,
      final TopicPartition changelogTopicPartition,
      final Consumer<Long> onCommit,
      final LongSupplier storedOffset,
      final BooleanSupplier maybeInitialize,
      final long startOffset
  ) {
    this.storeName = Objects.requireNonNull(storeName);
    this.changelogTopicPartition = Objects.requireNonNull(changelogTopicPartition);
    this.onCommit = Objects.requireNonNull(onCommit);
    this.storedOffset = Objects.requireNonNull(storedOffset);
    this.maybeInitialize = Objects.requireNonNull(maybeInitialize);
    this.startOffset = startOffset;
    this.log = new LogContext(
        String.format("registered changelog [%s]", changelogTopicPartition)
    ).logger(ResponsiveStoreRegistration.class);
    log.debug("Created store registration with stored offset={}", startOffset);
  }

  /**
   * Invoked prior to beginning restoration on a changelog, whether active or standby.
   * Used by the {@link dev.responsive.kafka.clients.ResponsiveRestoreConsumer} to determine
   * where to seek before starting the restore
   *
   * @return the initial committed offset from which restoration should be started
   */
  public long startOffset() {
    maybeInitializeStateStoreAndOffset();

    if (startOffset == NO_COMMITTED_OFFSET) {
      log.debug("Starting at offset 0 due to no prior committed offset");
      return 0L;
    } else if (startOffset == UNINITIALIZED_OFFSET) {
      log.debug("Returning dummy start offset 0 for un-initialized standby store");
      return 0L;
    } else {
      return startOffset;
    }
  }

  private void maybeInitializeStateStoreAndOffset() {
    if (startOffset == UNINITIALIZED_OFFSET) {
      if (maybeInitialize.getAsBoolean()) {
        startOffset = storedOffset.getAsLong();
        log.info("Initialized recycled state store with starting offset = {}", startOffset);
      }
    }
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
}
