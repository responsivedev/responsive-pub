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

package dev.responsive.kafka.api.async.internals.events;


/**
 * A record (or tombstone) that should be written to the given state store.
 * These records are created by the AsyncStateStore when a put is intercepted
 * by an AsyncThread. They are then added to the corresponding AsyncEvent, which
 * will handle the handoff from AsyncThread back to StreamThread, and ultimately
 * ensure all delayed writes are executed by that StreamThread during the
 * finalization phase of an async event.
 */
public class DelayedWrite<KS, VS> {

  private final KS recordKey;
  private final VS recordValue;
  private final String storeName;

  public DelayedWrite(
      final KS recordKey,
      final VS recordValue,
      final String storeName
  ) {
    this.recordKey = recordKey;
    this.recordValue = recordValue;
    this.storeName = storeName;
  }

  public String storeName() {
    return storeName;
  }

  public KS key() {
    return recordKey;
  }

  public VS value() {
    return recordValue;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final DelayedWrite<?, ?> that = (DelayedWrite<?, ?>) o;

    if (!recordKey.equals(that.recordKey)) {
      return false;
    }
    return storeName.equals(that.storeName);
  }

  @Override
  public int hashCode() {
    return 31 * recordKey.hashCode() + storeName.hashCode();
  }

  @Override
  public String toString() {
    return "DelayedWrite<" + recordKey + ", " + recordValue + ">";
  }
}
