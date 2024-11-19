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

plugins {
    id("responsive.java-application-conventions")
    id("responsive.docker")
}

application {
    mainClass.set("dev.responsive.examples.simpleapp.Main")
}

repositories {
    mavenLocal()
}

dependencies {
    // todo: how to set the version here?
    implementation(project(":kafka-client"))
    implementation("com.google.guava:guava:32.1.1-jre")
    implementation(libs.kafka.clients)
    implementation(libs.kafka.streams)
    implementation("io.opentelemetry.javaagent:opentelemetry-javaagent:1.25.0")
    implementation("org.apache.logging.log4j:log4j-slf4j-impl:2.20.0")
    implementation("org.apache.commons:commons-text:1.10.0")
    implementation("com.scylladb:java-driver-core:4.15.0.0")
    implementation("com.scylladb:java-driver-query-builder:4.15.0.0")
    implementation("com.scylladb:java-driver-mapper-runtime:4.15.0.0")
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(11))
    }
}

version = project(":kafka-client").version

responsive_docker.dockerImage.set("simple-example:$version")
