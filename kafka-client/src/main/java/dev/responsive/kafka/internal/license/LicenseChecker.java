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

import dev.responsive.kafka.internal.license.exception.LicenseUseViolationException;
import dev.responsive.kafka.internal.license.model.LicenseInfo;
import dev.responsive.kafka.internal.license.model.TimedTrialV1;
import dev.responsive.kafka.internal.license.model.UsageBasedV1;
import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LicenseChecker {

  private static final Logger LOG = LoggerFactory.getLogger(LicenseChecker.class);

  public void checkLicense(final LicenseInfo licenseInfo) {
    if (licenseInfo instanceof TimedTrialV1) {
      verifyTimedTrialV1((TimedTrialV1) licenseInfo);
      LOG.info("Checked and confirmed valid Time Trial license");
    } else if (licenseInfo instanceof UsageBasedV1) {
      LOG.info("Checked and confirmed valid Usage Based license");
    } else {
      throw new IllegalArgumentException(
          "unsupported license type: " + licenseInfo.getClass().getName());
    }
  }

  private void verifyTimedTrialV1(final TimedTrialV1 timedTrial) {
    final Instant expiresAt = Instant.ofEpochSecond(timedTrial.expiresAt());
    if (Instant.now().isAfter(expiresAt)) {
      throw new LicenseUseViolationException("license expired at: " + expiresAt);
    }
  }
}
