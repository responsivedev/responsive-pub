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

import dev.responsive.kafka.internal.license.exception.LicenseUseViolationException;
import dev.responsive.kafka.internal.license.model.LicenseInfo;
import dev.responsive.kafka.internal.license.model.TimedTrialV1;
import java.time.Instant;

public class LicenseChecker {
  public void checkLicense(final LicenseInfo licenseInfo) {
    if (licenseInfo instanceof TimedTrialV1) {
      verifyTimedTrialV1((TimedTrialV1) licenseInfo);
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
