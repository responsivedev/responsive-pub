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

package dev.responsive.tools;

import dev.responsive.tools.Visitors.FilterMethodCollector;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.concurrent.Callable;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import org.objectweb.asm.ClassReader;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

@Command(
    name = "analyze",
    mixinStandardHelpOptions = true,
    description = "analyzes a JAR file for Kafka Stream usage"
)
public class StreamsBytecodeAnalyzer implements Callable<Integer> {

  @Parameters(index = "0", description = "Path to JAR File")
  private File jarPath;

  @Override
  public Integer call() throws Exception {
    final FilterMethodCollector methods = new FilterMethodCollector(
        owner -> owner.contains("org/apache/kafka/streams")
    );

    try (JarFile jar = new JarFile(jarPath)) {
      Enumeration<JarEntry> entries = jar.entries();
      while (entries.hasMoreElements()) {
        JarEntry entry = entries.nextElement();

        if (entry.getName().endsWith(".class")) {
          InputStream stream = new BufferedInputStream(jar.getInputStream(entry), 1024);
          ClassReader reader = new ClassReader(stream);

          reader.accept(new Visitors.DelegatingClassVisitor(methods), 0);

          stream.close();
        }
      }
    }

    methods.describe();
    return 0;
  }

  public static void main(String... args) {
    int exitCode = new CommandLine(new StreamsBytecodeAnalyzer()).execute(args);
    System.exit(exitCode);
  }
}