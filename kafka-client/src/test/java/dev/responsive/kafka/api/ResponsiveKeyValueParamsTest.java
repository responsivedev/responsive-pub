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

import dev.responsive.kafka.api.stores.ResponsiveKeyValueParams;
import dev.responsive.kafka.api.stores.TtlProvider;
import java.time.Duration;
import java.util.Optional;
import org.junit.jupiter.api.Test;

public class ResponsiveKeyValueParamsTest {

  private static final Duration DEFAULT_TTL = Duration.ofSeconds(5L);

  @Test
  public void shouldConvertWithTimeToLiveToTtlProvider() {
    // Given/when:
    final var params = ResponsiveKeyValueParams.fact("store")
        .withTimeToLive(DEFAULT_TTL);

    // Then:
    assertThat(params.ttlProvider(), equalTo(Optional.of(TtlProvider.withDefault(DEFAULT_TTL))));
  }

  @Test
  public void shouldConvertInfiniteDefaultOnlyTtlProviderToOptionalEmpty() {
    // Given/when:
    final var params = ResponsiveKeyValueParams.fact("store")
            .withTtlProvider(TtlProvider.withNoDefault());

    // Then:
    assertThat(params.ttlProvider(), equalTo(Optional.empty()));
  }

}
