/*
 * Copyright 2024 Responsive Computing, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package dev.responsive.kafka.api.async.internals;

import java.util.Comparator;

/**
 * Simple container class for info that uniquely identifies a {@link AsyncProcessor} instance
 */
public class AsyncProcessorId implements Comparable<AsyncProcessorId> {

  public final String processorName;
  public final int partition;

  public static AsyncProcessorId of(final String processorName, final int partition) {
    return new AsyncProcessorId(processorName, partition);
  }

  private AsyncProcessorId(final String processorName, final int partition) {
    this.processorName = processorName;
    this.partition = partition;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final AsyncProcessorId that = (AsyncProcessorId) o;

    if (partition != that.partition) {
      return false;
    }
    return processorName.equals(that.processorName);
  }

  @Override
  public int hashCode() {
    int result = processorName.hashCode();
    result = 31 * result + partition;
    return result;
  }

  @Override
  public String toString() {
    return "AsyncProcessorId<" + processorName + "_" + partition + '>';
  }

  @Override
  public int compareTo(final AsyncProcessorId o) {
    final int comparingName = this.processorName.compareTo(o.processorName);
    return comparingName != 0 ? comparingName : this.partition - o.partition;
  }
}
