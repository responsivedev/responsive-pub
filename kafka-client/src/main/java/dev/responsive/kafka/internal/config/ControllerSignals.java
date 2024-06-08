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

package dev.responsive.kafka.internal.config;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Container class for signals and requests from the controller, such as manual rebalances
 *
 * TODO: connect to background thread that listens for requests and interfaces with controller
 */
public class ControllerSignals {

  private final AtomicBoolean rebalanceRequested = new AtomicBoolean(false);

  public AtomicBoolean rebalanceRequested() {
    return rebalanceRequested;
  }
}
