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