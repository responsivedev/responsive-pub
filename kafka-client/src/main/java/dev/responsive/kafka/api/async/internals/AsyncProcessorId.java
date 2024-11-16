/*
 * Copyright 2024 Responsive Computing, Inc.
 *
 * This source code is licensed under the Responsive Software License Agreement v1.0 available at
 *
 * https://www.responsive.dev/legal/responsive-bsl-10
 *
 * This software requires a valid Commercial License Key for production use. Trial and commercial
 * licenses can be obtained at https://www.responsive.dev/sdk/get-started.
 */

package dev.responsive.kafka.api.async.internals;

import org.apache.kafka.streams.processor.TaskId;

/**
 * Simple container class for info that uniquely identifies a {@link AsyncProcessor} instance
 */
public class AsyncProcessorId implements Comparable<AsyncProcessorId> {

  public final String processorName;
  public final TaskId taskId;

  public static AsyncProcessorId of(final String processorName, final TaskId taskId) {
    return new AsyncProcessorId(processorName, taskId);
  }

  private AsyncProcessorId(final String processorName, final TaskId taskId) {
    this.processorName = processorName;
    this.taskId = taskId;
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

    if (!taskId.equals(that.taskId)) {
      return false;
    }
    return processorName.equals(that.processorName);
  }

  @Override
  public int hashCode() {
    int result = processorName.hashCode();
    result = 31 * result + taskId.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "AsyncProcessorId<" + processorName + "_" + taskId + '>';
  }

  @Override
  public int compareTo(final AsyncProcessorId o) {
    final int comparingName = this.processorName.compareTo(o.processorName);
    return comparingName != 0 ? comparingName : this.taskId.compareTo(o.taskId);
  }
}
