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

    public FilterMethodCollector(
        final Predicate<String> ownerFilter
    ) {
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
        final boolean isInterface
    ) {
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
      methods.forEach((k, v) -> {
        System.out.println(k + " -> " + v);
      });
    }
  }

  private static String sanitize(final String name, final String desc) {
    // the description has all the arguments in between () separated
    // by semi-colons
    String[] args = desc.substring(
        desc.indexOf('(') + 1,
        desc.lastIndexOf(')')
    ).split(";");

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
