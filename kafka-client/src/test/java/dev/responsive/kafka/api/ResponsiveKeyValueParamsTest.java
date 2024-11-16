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
