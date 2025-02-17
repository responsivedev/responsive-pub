/*
 * Copyright 2024 Responsive Computing, Inc.
 *
 * This source code is licensed under the Responsive Business Source License Agreement v1.0
 * available at:
 *
 * https://www.responsive.dev/legal/responsive-bsl-10
 *
 * This software requires a valid Commercial License Key for production use. Trial and commercial
 * licenses can be obtained at https://www.responsive.dev
 */

package dev.responsive.kafka.internal.license;

import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.responsive.kafka.internal.license.exception.LicenseUseViolationException;
import dev.responsive.kafka.internal.license.model.LicenseInfo;
import dev.responsive.kafka.internal.license.model.TimedTrialV1;
import dev.responsive.kafka.internal.license.model.UsageBasedV1;
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

  @Test
  public void shouldAcceptValidUsageBasedLicense() {
    // given:
    final LicenseInfo info = new UsageBasedV1(
        UsageBasedV1.TYPE_NAME,
        "foo@bar.com",
        "key",
        "secret"
    );

    // when/then (no throw):
    checker.checkLicense(info);
  }
}