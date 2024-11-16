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
    id("responsive.docker")
}

application {
    mainClass.set("dev.responsive.examples.e2etest.Main")
}

dependencies {
    // todo: how to set the version here?
    implementation(project(":kafka-client"))
    implementation(libs.kafka.clients)
    implementation(libs.kafka.streams)

    implementation("com.antithesis:sdk:1.3.1")
    implementation("org.apache.commons:commons-text:1.10.0")

    implementation(libs.guava)
    implementation(libs.slf4j.log4j2)
    implementation(libs.bundles.scylla)
    implementation(libs.jackson)
    implementation(libs.mongodb.driver.core)

    testImplementation(testlibs.bundles.base)
    testImplementation(testlibs.bundles.testcontainers)
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(17))
    }
}

fun String.runCommand(wd: File = file("./")): String {
    val byteOut = `java.io`.ByteArrayOutputStream()
    project.exec {
        workingDir = wd
        commandLine = this@runCommand.split("\\s".toRegex())
        standardOutput = byteOut
    }
    return String(byteOut.toByteArray()).trim()
}

val kc_version = project(":kafka-client").version
version = "$kc_version" + "-" + "git rev-parse --short HEAD".runCommand(wd = rootDir)

responsive_docker.dockerImage.set("e2e-test:$version")
