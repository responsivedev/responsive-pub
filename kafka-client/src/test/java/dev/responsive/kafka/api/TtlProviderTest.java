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

package dev.responsive.kafka.api;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.responsive.kafka.api.stores.TtlProvider;
import dev.responsive.kafka.api.stores.TtlProvider.TtlDuration;
import dev.responsive.kafka.internal.utils.StateDeserializer;
import java.time.Duration;
import java.util.Optional;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.junit.jupiter.api.Test;

public class TtlProviderTest {

  private static final Deserializer<Object> THROWING_DESERIALIZER = (t, d) -> {
    throw new AssertionError("this deserializer should not be invoked");
  };

  @Test
  public void shouldReturnDefaultOnlyTtlProvider() {
    // Given/when:
    final var ttlProvider = TtlProvider.withDefault(Duration.ofSeconds(5L));

    //Then:
    assertThat(ttlProvider.hasDefaultOnly(), is(true));
    assertThat(ttlProvider.needsValueToComputeTtl(), is(false));
    assertThat(ttlProvider.defaultTtl(), equalTo(TtlDuration.of(Duration.ofSeconds(5L))));

    final var defaultOnlyStateDeserializer = new StateDeserializer<>(
        null,
        THROWING_DESERIALIZER,
        THROWING_DESERIALIZER
    );
    assertThat(
        ttlProvider.computeTtl(null, null, defaultOnlyStateDeserializer),
        equalTo(Optional.empty())
    );
  }

  @Test
  public void shouldReturnInfiniteDefaultOnlyTtlProvider() {
    // Given/when:
    final var ttlProvider = TtlProvider.withNoDefault();

    //Then:
    assertThat(ttlProvider.hasDefaultOnly(), is(true));
    assertThat(ttlProvider.needsValueToComputeTtl(), is(false));
    assertThat(ttlProvider.defaultTtl(), equalTo(TtlDuration.infinite()));

    final var defaultOnlyStateDeserializer = new StateDeserializer<>(
        null,
        THROWING_DESERIALIZER,
        THROWING_DESERIALIZER
    );
    assertThat(
        ttlProvider.computeTtl(null, null, defaultOnlyStateDeserializer),
        equalTo(Optional.empty())
    );
  }

  @Test
  public void shouldReturnKeyOnlyTtlProvider() {
    // Given/when:
    final TtlProvider<byte[], Object> ttlProvider = TtlProvider.<byte[], Object>withNoDefault()
        .fromKey(k -> Optional.of(TtlDuration.of(Duration.ofSeconds(5L))));

    //Then:
    assertThat(ttlProvider.hasDefaultOnly(), is(false));
    assertThat(ttlProvider.needsValueToComputeTtl(), is(false));
    assertThat(ttlProvider.defaultTtl(), equalTo(TtlDuration.infinite()));

    final var keyOnlyStateDeserializer = new StateDeserializer<>(
        null,
        new ByteArrayDeserializer(),
        THROWING_DESERIALIZER
    );
    assertThat(
        ttlProvider.computeTtl(new byte[]{}, null, keyOnlyStateDeserializer),
        equalTo(Optional.of(TtlDuration.of(Duration.ofSeconds(5L))))
    );


    // if key is not provided this should throw
    assertThrows(
        IllegalStateException.class,
        () -> ttlProvider.computeTtl(null, null, keyOnlyStateDeserializer)
    );
  }

  @Test
  public void shouldReturnValueOnlyTtlProvider() {
    // Given/when:
    final var ttlProvider = TtlProvider.<Object, byte[]>withNoDefault()
        .fromValue(k -> Optional.of(TtlDuration.of(Duration.ofSeconds(5L))));

    //Then:
    assertThat(ttlProvider.hasDefaultOnly(), is(false));
    assertThat(ttlProvider.needsValueToComputeTtl(), is(true));
    assertThat(ttlProvider.defaultTtl(), equalTo(TtlDuration.infinite()));

    final var valueOnlyStateDeserializer = new StateDeserializer<>(
        null,
        THROWING_DESERIALIZER,
        new ByteArrayDeserializer()
    );
    assertThat(
        ttlProvider.computeTtl(null, new byte[]{}, valueOnlyStateDeserializer),
        equalTo(Optional.of(TtlDuration.of(Duration.ofSeconds(5L))))
    );

    // if value is not provided this should throw
    assertThrows(
        IllegalStateException.class,
        () -> ttlProvider.computeTtl(null, null, valueOnlyStateDeserializer)
    );
  }

  @Test
  public void shouldReturnKeyAndValueTtlProvider() {
    // Given/when:
    final var ttlProvider = TtlProvider.<byte[], byte[]>withNoDefault()
        .fromKeyAndValue((k, v) -> Optional.of(TtlDuration.of(Duration.ofSeconds(5L))));

    //Then:
    assertThat(ttlProvider.hasDefaultOnly(), is(false));
    assertThat(ttlProvider.needsValueToComputeTtl(), is(true));
    assertThat(ttlProvider.defaultTtl(), equalTo(TtlDuration.infinite()));

    final var keyAndValueStateDeserializer = new StateDeserializer<>(
        null,
        new ByteArrayDeserializer(),
        new ByteArrayDeserializer()
    );
    assertThat(
        ttlProvider.computeTtl(new byte[]{}, new byte[]{}, keyAndValueStateDeserializer),
        equalTo(Optional.of(TtlDuration.of(Duration.ofSeconds(5L))))
    );

    // if key OR value is not provided this should throw
    assertThrows(
        IllegalStateException.class,
        () -> ttlProvider.computeTtl(new byte[]{}, null, keyAndValueStateDeserializer)
    );
    assertThrows(
        IllegalStateException.class,
        () -> ttlProvider.computeTtl(null, new byte[]{}, keyAndValueStateDeserializer)
    );
  }

  @Test
  public void shouldConvertFiniteTtlDuration() {
    // Given/when:
    final var ttlDuration = TtlDuration.of(Duration.ofSeconds(1L));

    //Then:
    assertThat(ttlDuration.isFinite(), is(true));
    assertThat(ttlDuration.toSeconds(), equalTo(1L));
    assertThat(ttlDuration.toMillis(), equalTo(1000L));
    assertThat(ttlDuration.duration(), equalTo(Duration.ofSeconds(1L)));

    final var equivalentTtlDuration = TtlDuration.of(Duration.ofMillis(1000L));

    assertThat(ttlDuration, equalTo(equivalentTtlDuration));
  }

  @Test
  public void shouldNotConvertInfiniteTtlDuration() {
    // Given/when:
    final var ttlDuration = TtlDuration.infinite();

    //Then:
    assertThat(ttlDuration.isFinite(), is(false));
    assertThrows(IllegalStateException.class, ttlDuration::toSeconds);
    assertThrows(IllegalStateException.class, ttlDuration::toMillis);
    assertThrows(IllegalStateException.class, ttlDuration::duration);

    final var equivalentTtlDuration = TtlDuration.infinite();

    assertThat(ttlDuration, equalTo(equivalentTtlDuration));
  }

}
