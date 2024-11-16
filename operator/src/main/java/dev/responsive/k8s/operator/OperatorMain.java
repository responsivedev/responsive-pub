/*
 * Copyright 2024 Responsive Computing, Inc.
 *
 * This source code is licensed under the Responsive Software License Agreement v1.0 available at
 *
 * https://www.responsive.dev/legal/responsive-bsl-10
 *
 * This software requires a valid Commercial License Key for production use. Trial and commercial
 * licenses can be obtained at https://www.responsive.dev/sdk/get-started.
 */

package dev.responsive.k8s.operator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import dev.responsive.controller.client.grpc.ControllerGrpcClient;
import dev.responsive.k8s.operator.reconciler.ResponsivePolicyReconciler;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.utils.KubernetesSerialization;
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

  // copied from io.javaoperatorsdk.operator.api.config.ConfigurationService
  private static final int DEFAULT_MAX_CONCURRENT_REQUEST = 512;

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
    final String environment = cmd.hasOption(OperatorOptions.ENVIRONMENT)
        ? cmd.getOptionValue(OperatorOptions.ENVIRONMENT) : "";
    final String secretFilePath = cmd.getOptionValue(OperatorOptions.SECRETS_FILE);
    final boolean tlsOff = cmd.hasOption(OperatorOptions.TLS_OFF);
    final String selector = cmd.getOptionValue(OperatorOptions.LABEL_SELECTOR);

    final Properties config = load(secretFilePath);

    if (!(config.containsKey(API_KEY_CONFIG) && config.containsKey(SECRET_CONFIG))) {
      LOG.error("Couldn't find API Key or secret properties in config file {}. "
              + "We expect both {} and {} properties to be present", secretFilePath,
              API_KEY_CONFIG, SECRET_CONFIG);
      System.exit(1);
    }

    final String apiKey = config.getProperty(API_KEY_CONFIG, "");
    final String secret = config.getProperty(SECRET_CONFIG, "");

    final ObjectMapper mapper = new ObjectMapper();
    mapper.registerModule(new Jdk8Module());
    // we override the k8s client here so we can supply our own ObjectMapper
    final Operator operator = new Operator(o -> o
        .withSSABasedCreateUpdateMatchForDependentResources(false)
        .withKubernetesClient(
            new KubernetesClientBuilder()
                // copied from io.javaoperatorsdk.operator.api.config.ConfigurationService
                .withConfig(new ConfigBuilder(Config.autoConfigure(null))
                    .withMaxConcurrentRequests(DEFAULT_MAX_CONCURRENT_REQUEST)
                    .build())
                .withKubernetesSerialization(new KubernetesSerialization(mapper, true))
                .build()
    ));
    final ResponsivePolicyReconciler reconciler =
        new ResponsivePolicyReconciler(environment, new ControllerGrpcClient(
            target,
            apiKey,
            secret,
            tlsOff
        ));

    operator.register(reconciler, o -> o.withLabelSelector(selector));

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
