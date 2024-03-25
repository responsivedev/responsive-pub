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

/**
 * Simple container class for metadata that uniquely identifies a specific
 * physical AsyncProcessor instance within a single StreamThread
 */
public final class AsyncNodeId {
  private final String asyncProcessorName;
  private final int partition;

  public AsyncNodeId(final String asyncProcessorName, final int partition) {
    this.asyncProcessorName = asyncProcessorName;
    this.partition = partition;
  }

  public String asyncProcessorName() {
    return asyncProcessorName;
  }

  public int partition() {
    return partition;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final AsyncNodeId that = (AsyncNodeId) o;

    if (partition != that.partition) {
      return false;
    }
    return asyncProcessorName.equals(that.asyncProcessorName);
  }

  @Override
  public int hashCode() {
    int result = asyncProcessorName.hashCode();
    result = 31 * result + partition;
    return result;
  }
}
