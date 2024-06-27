package dev.responsive.kafka.internal.db.testutils;

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
