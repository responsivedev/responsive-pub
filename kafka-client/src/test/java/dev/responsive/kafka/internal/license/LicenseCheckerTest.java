/*
 * Copyright 2024 Responsive Computing, Inc.
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

package dev.responsive.kafka.internal.license;

import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.responsive.kafka.internal.license.exception.LicenseUseViolationException;
import dev.responsive.kafka.internal.license.model.LicenseInfo;
import dev.responsive.kafka.internal.license.model.TimedTrialV1;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class LicenseCheckerTest {
  private final LicenseChecker checker = new LicenseChecker();

  @Test
  public void shouldThrowOnExpiredTrialV1License() {
    // given:
    final LicenseInfo info = new TimedTrialV1(
        "timed_trial_v1",
        "foo@bar.com",
        0,
        Instant.now().minus(Duration.ofHours(1)).getEpochSecond()
    );

    // when/then:
    assertThrows(LicenseUseViolationException.class, () -> checker.checkLicense(info));
  }

  @Test
  public void shouldAcceptValidTrialV1License() {
    // given:
    final LicenseInfo info = new TimedTrialV1(
        "timed_trial_v1",
        "foo@bar.com",
        0,
        Instant.now().plus(Duration.ofHours(1)).getEpochSecond()
    );

    // when/then (no throw):
    checker.checkLicense(info);
  }
}