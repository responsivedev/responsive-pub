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

package dev.responsive.k8s.operator;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public final class OperatorOptions {

  private OperatorOptions() {}

  public static final Option CONTROLLER_URL = Option.builder("controllerUrl")
      .hasArg(true)
      .required(true)
      .desc("The URL for the controller server")
      .numberOfArgs(1)
      .build();

  public static final Option SECRETS_FILE = Option.builder("secretsFile")
      .hasArg(true)
      .required(true)
      .desc("The path to the secrets file")
      .numberOfArgs(1)
      .build();

  public static final Options OPTIONS = new Options()
      .addOption(CONTROLLER_URL)
      .addOption(SECRETS_FILE);

}
