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

import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import dev.responsive.controller.client.grpc.ControllerGrpcClient;
import dev.responsive.k8s.operator.reconciler.ResponsivePolicyReconciler;
import io.fabric8.kubernetes.client.utils.Serialization;
import io.javaoperatorsdk.operator.Operator;
import java.io.File;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OperatorMain {
  private static final Logger LOGGER = LoggerFactory.getLogger(OperatorMain.class);
  private static final String API_KEY_CONFIG = "responsive.platform.api.key";
  private static final String SECRET_CONFIG = "responsive.platform.api.secret";

  public static void main(String[] args) {

    final Options options = getOptions();
    final CommandLineParser parser = new DefaultParser();
    final HelpFormatter formatter = new HelpFormatter();

    CommandLine cmd;

    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      LOGGER.error("Error parsing command line params: ", e);
      formatter.printHelp("Operator", options);
      System.exit(1);
      return;
    }

    final String target = cmd.getOptionValue("server-url");
    final String configFileLocation = cmd.getOptionValue("config-file");
    final Configuration config = getConfigurations(configFileLocation);

    if (!(config.containsKey(API_KEY_CONFIG) && config.containsKey(SECRET_CONFIG))) {
      LOGGER.error("Couldn't find API Key or secret properties in config file {}. "
              + "We expect both {} and {} properties to be present", configFileLocation,
              API_KEY_CONFIG, SECRET_CONFIG);
      System.exit(1);
    }

    final String apiKey = config.getString(API_KEY_CONFIG, "");
    final String secret = config.getString(SECRET_CONFIG, "");

    final Operator operator = new Operator();
    Serialization.jsonMapper().registerModule(new Jdk8Module());
    operator.register(new ResponsivePolicyReconciler(new ControllerGrpcClient(target,
            apiKey, secret)));
    operator.start();
  }

  private static Configuration getConfigurations(String fileName) {
    Configurations configurations = new Configurations();
    try {
      return configurations.properties(new File(fileName));
    } catch (Exception e) {
      LOGGER.error("Error loading configuration properties: {}", e.getMessage());
    }
    return new PropertiesConfiguration();
  }

  private static Options getOptions() {
    Options options = new Options();
    Option serverOption = Option.builder("controllerUrl")
            .hasArg(true)
            .required(true)
            .desc("The URL for the controller server")
            .numberOfArgs(1)
            .build();

    options.addOption(serverOption);

    Option configFile = Option.builder("configFile")
            .hasArg(true)
            .required(true)
            .desc("The path to the config file")
            .numberOfArgs(1)
            .build();

    options.addOption(configFile);

    return options;

  }
}
