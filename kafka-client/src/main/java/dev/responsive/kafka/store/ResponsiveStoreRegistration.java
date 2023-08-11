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

import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import java.util.Objects;
import java.util.function.Consumer;
import org.apache.kafka.common.TopicPartition;

public final class ResponsiveStoreRegistration {
  private final TopicPartition changelogTopicPartition;
  private final long committedOffset;
  private final String name;
  private final Consumer<Long> onCommit;

  @VisibleForTesting
  public ResponsiveStoreRegistration(
      final String name,
      final TopicPartition changelogTopicPartition,
      final long committedOffset,
      final Consumer<Long> onCommit
  ) {
    this.name = Objects.requireNonNull(name);
    this.changelogTopicPartition = Objects.requireNonNull(changelogTopicPartition);
    this.committedOffset = committedOffset;
    this.onCommit = Objects.requireNonNull(onCommit);
  }

  public long getCommittedOffset() {
    return committedOffset;
  }

  public TopicPartition getChangelogTopicPartition() {
    return changelogTopicPartition;
  }

  public String getName() {
    return name;
  }

  public Consumer<Long> getOnCommit() {
    return onCommit;
  }
}
