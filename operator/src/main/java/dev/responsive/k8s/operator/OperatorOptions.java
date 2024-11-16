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

  public static final Option ENVIRONMENT = Option.builder("environment")
      .hasArg(true)
      .required(false)
      .desc("The environment this operator is running in")
      .numberOfArgs(1)
      .build();

  public static final Option SECRETS_FILE = Option.builder("secretsFile")
      .hasArg(true)
      .required(true)
      .desc("The path to the secrets file")
      .numberOfArgs(1)
      .build();

  public static final Option LABEL_SELECTOR = Option.builder("labelSelector")
      .hasArg(true)
      .required(false)
      .desc("If specified, only policies that match this label selector will be resolved.")
      .numberOfArgs(1)
      .build();

  public static final Option TLS_OFF =
      new Option("disableTls", "Disable TLS connection to responsive");

  public static final Options OPTIONS = new Options()
      .addOption(CONTROLLER_URL)
      .addOption(SECRETS_FILE)
      .addOption(TLS_OFF)
      .addOption(ENVIRONMENT)
      .addOption(LABEL_SELECTOR);
}
