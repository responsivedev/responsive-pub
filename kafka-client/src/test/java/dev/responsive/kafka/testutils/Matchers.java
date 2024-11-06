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

package dev.responsive.kafka.testutils;

import java.util.Arrays;
import org.apache.kafka.streams.KeyValue;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;

public class Matchers {


  private Matchers() {
  }

  public static <T> Matcher<KeyValue<T, byte[]>> sameKeyValue(final KeyValue<T, byte[]> expected) {
    return new SameKeyValue<>(expected);
  }

  private static class SameKeyValue<T> extends BaseMatcher<KeyValue<T, byte[]>> {
    private final KeyValue<T, byte[]> expected;

    public SameKeyValue(KeyValue<T, byte[]> expected) {
      this.expected = expected;
    }

    @Override
    public boolean matches(Object o) {
      if (!(o instanceof KeyValue)) {
        return false;
      }
      final KeyValue<?, ?> otherKeyValue = (KeyValue<?, ?>) o;
      return expected.key.equals(otherKeyValue.key)
          && otherKeyValue.value instanceof byte[]
          && Arrays.equals(expected.value, (byte[]) otherKeyValue.value);
    }

    @Override
    public void describeTo(Description description) {
      description.appendValue(expected);
    }
  }
}
