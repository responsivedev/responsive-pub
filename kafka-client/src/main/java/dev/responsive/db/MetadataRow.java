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

package dev.responsive.db;

import java.util.Objects;

public class MetadataRow {

  public final long offset;
  public final long epoch;

  public MetadataRow(final long offset, final long epoch) {
    this.offset = offset;
    this.epoch = epoch;
  }

  @Override
  public String toString() {
    return "MetadataRow{"
        + "offset=" + offset
        + ", epoch=" + epoch
        + '}';
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final MetadataRow that = (MetadataRow) o;
    return offset == that.offset && epoch == that.epoch;
  }

  @Override
  public int hashCode() {
    return Objects.hash(offset, epoch);
  }
}
