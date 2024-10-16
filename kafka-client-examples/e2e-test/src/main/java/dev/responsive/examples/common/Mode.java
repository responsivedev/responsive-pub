/*
 * Copyright 2023 Responsive Computing, Inc.
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

package dev.responsive.examples.common;

public enum Mode {
  DRIVER,
  APPLICATION,

  REGRESSION_ST_DRIVER,
  REGRESSION_ST_JOIN,
  REGRESSION_ST_BASELINE,

  REGRESSION_BATCH_DRIVER,
  REGRESSION_BATCH,
  REGRESSION_BATCH_BASELINE,
}
