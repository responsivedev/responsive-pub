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
    description = "analyzes a JAR file for Kafka Stream usage")
public class StreamsBytecodeAnalyzer implements Callable<Integer> {

  @Parameters(index = "0", description = "Path to JAR File")
  private File jarPath;

  @Override
  public Integer call() throws Exception {
    final FilterMethodCollector methods =
        new FilterMethodCollector(owner -> owner.contains("org/apache/kafka/streams"));

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
