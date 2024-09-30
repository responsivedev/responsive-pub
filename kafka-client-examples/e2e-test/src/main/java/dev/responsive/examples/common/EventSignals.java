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

package dev.responsive.examples.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Common log lines for signalling events to Antithesis from a common Logger (ie class name)
 *
 */
public class EventSignals {

  private static final Logger LOG = LoggerFactory.getLogger(EventSignals.class);

  public static void logNumConsumedOutputRecords(final int numRecordsConsumed) {
    LOG.info("Consumed {} output records", numRecordsConsumed);
  }

}
