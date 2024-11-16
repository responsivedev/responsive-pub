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

plugins {
    id("responsive.java-application-conventions")
}

dependencies {
    // don't include these in libs since they are specific to only this
    // tooling module and may cause issues if included in other jars
    implementation("org.ow2.asm:asm:9.4")
    implementation("info.picocli:picocli:4.7.4")

    annotationProcessor("info.picocli:picocli-codegen:4.7.4")
}

val fatJar = task("fatJar", type = Jar::class) {
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    archiveBaseName.set("${project.name}-fat")
    manifest {
        attributes["Implementation-Title"] = "Responsive Analyzer Tool"
        attributes["Implementation-Version"] = "0.1.0"
    }
    from(configurations.runtimeClasspath.get().map { if (it.isDirectory) it else zipTree(it) })
    with(tasks.jar.get() as CopySpec)
}

tasks.withType<JavaCompile> {
    options.compilerArgs.addAll(arrayOf(
        "-Xlint:-processing",
        "-Aproject=${project.group}/${project.name}"
    ))
}

tasks {
    "build" {
        dependsOn(fatJar)
    }
}