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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

public final class Visitors {

  public static class DelegatingClassVisitor extends ClassVisitor {

    private final MethodVisitor delegate;

    public DelegatingClassVisitor(final MethodVisitor delegate) {
      super(Opcodes.ASM9);
      this.delegate = delegate;
    }

    @Override
    public MethodVisitor visitMethod(
        final int access,
        final String name,
        final String desc,
        final String signature,
        final String[] exceptions) {
      return delegate;
    }
  }

  public static class FilterMethodCollector extends MethodVisitor {

    private final Map<String, Set<String>> methods;
    private final Predicate<String> ownerFilter;

    public FilterMethodCollector(final Predicate<String> ownerFilter) {
      super(Opcodes.ASM9);
      this.ownerFilter = ownerFilter;
      this.methods = new HashMap<>();
    }

    @Override
    public void visitMethodInsn(
        final int opcode,
        final String owner,
        final String name,
        final String descriptor,
        final boolean isInterface) {
      if (ownerFilter.test(owner)) {
        methods.compute(
            owner,
            (k, v) -> {
              final Set<String> result = Objects.requireNonNullElseGet(v, HashSet::new);
              result.add(sanitize(name, descriptor));
              return result;
            });
      }
    }

    public void describe() {
      methods.forEach(
          (k, v) -> {
            System.out.println(k + " -> " + v);
          });
    }
  }

  private static String sanitize(final String name, final String desc) {
    // the description has all the arguments in between () separated
    // by semi-colons
    String[] args = desc.substring(desc.indexOf('(') + 1, desc.lastIndexOf(')')).split(";");

    // each arg contains the full package name, for simplicity we want
    // to just return the name of the class
    for (int i = 0; i < args.length; i++) {
      final String arg = args[i];
      if (arg.lastIndexOf('/') >= 0) {
        args[i] = arg.substring(arg.lastIndexOf('/') + 1);
      }
    }

    return name + "(" + String.join(", ", args) + ")";
  }
}
