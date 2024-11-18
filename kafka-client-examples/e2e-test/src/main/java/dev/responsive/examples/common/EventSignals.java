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
