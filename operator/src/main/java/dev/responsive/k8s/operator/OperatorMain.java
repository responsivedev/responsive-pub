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
import java.io.FileReader;
import java.io.Reader;
import java.util.Properties;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OperatorMain {
  private static final Logger LOG = LoggerFactory.getLogger(OperatorMain.class);
  private static final String API_KEY_CONFIG = "responsive.platform.api.key";
  private static final String SECRET_CONFIG = "responsive.platform.api.secret";

  public static void main(String[] args) {
    LOG.info("Starting main");

    final Options options = OperatorOptions.OPTIONS;
    final CommandLineParser parser = new DefaultParser();
    final HelpFormatter formatter = new HelpFormatter();

    CommandLine cmd;

    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      LOG.error("Error parsing command line params: ", e);
      formatter.printHelp("Operator", options);
      System.exit(1);
      return;
    }

    final String target = cmd.getOptionValue(OperatorOptions.CONTROLLER_URL);
    final String secretFilePath = cmd.getOptionValue(OperatorOptions.SECRETS_FILE);
    final Properties config = load(secretFilePath);

    if (!(config.containsKey(API_KEY_CONFIG) && config.containsKey(SECRET_CONFIG))) {
      LOG.error("Couldn't find API Key or secret properties in config file {}. "
              + "We expect both {} and {} properties to be present", secretFilePath,
              API_KEY_CONFIG, SECRET_CONFIG);
      System.exit(1);
    }

    final String apiKey = config.getProperty(API_KEY_CONFIG, "");
    final String secret = config.getProperty(SECRET_CONFIG, "");

    final Operator operator = new Operator();
    Serialization.jsonMapper().registerModule(new Jdk8Module());
    operator.register(new ResponsivePolicyReconciler(new ControllerGrpcClient(target,
            apiKey, secret)));
    operator.start();
  }

  private static Properties load(final String fileName) {
    final Properties properties = new Properties();
    try {
      final Reader reader = new FileReader(fileName);
      properties.load(reader);
    } catch (Exception e) {
      LOG.error("Error loading configuration properties.", e);
    }
    return properties;
  }
}
